package com.stroku.p2p.engine;

import com.stroku.p2p.core.Json;
import com.stroku.p2p.core.TorrentBackend;

import org.libtorrent4j.FileStorage;
import org.libtorrent4j.PeerRequest;
import org.libtorrent4j.Priority;
import org.libtorrent4j.SessionManager;
import org.libtorrent4j.Sha1Hash;
import org.libtorrent4j.TorrentFlags;
import org.libtorrent4j.TorrentHandle;
import org.libtorrent4j.TorrentInfo;
import org.libtorrent4j.TorrentStatus;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Motor torrent real (libtorrent4j, biblioteca nativa incluida en el APK).
 * Descarga los trozos que necesita la reproducción con prioridad por plazo (setPieceDeadline)
 * y los lee del disco solo cuando libtorrent confirma que el trozo está completo y verificado.
 */
public final class TorrentEngine implements TorrentBackend {
    private static final String[] VIDEO_EXT = {
            ".mp4", ".mkv", ".avi", ".mov", ".m4v", ".webm", ".ts", ".m2ts", ".wmv", ".mpg", ".mpeg"};
    private static final int READAHEAD_PIECES = 24;

    private final File saveDir;
    private final long resolveTimeoutMs;
    private final long pieceTimeoutMs;
    private final SessionManager session = new SessionManager();
    private final ConcurrentHashMap<String, Boolean> configured = new ConcurrentHashMap<>();
    private volatile boolean running;
    private volatile String lastError;

    public TorrentEngine(File saveDir, long resolveTimeoutMs, long pieceTimeoutMs) {
        this.saveDir = saveDir;
        this.resolveTimeoutMs = resolveTimeoutMs;
        this.pieceTimeoutMs = pieceTimeoutMs;
    }

    /** Arranca la sesión de libtorrent. Lanza excepción (incluido UnsatisfiedLinkError) si falla. */
    public void start() throws IOException {
        if (!saveDir.isDirectory() && !saveDir.mkdirs()) throw new IOException("no se pudo crear " + saveDir);
        deleteChildren(saveDir); // sesión limpia: no se acumulan películas antiguas en el teléfono
        session.start();
        if (!session.isRunning()) throw new IOException("libtorrent no arrancó");
        session.maxActiveDownloads(50);
        running = true;
    }

    public boolean isRunning() {
        return running && session.isRunning();
    }

    // ------------------------------------------------------------------ resolve

    @Override
    public Meta resolve(String hash, String magnet, int fileIndex) throws IOException {
        if (!isRunning()) throw new IOException("el motor torrent no está en marcha");
        Sha1Hash sha = Sha1Hash.fromBytes(hexToBytes(hash));
        TorrentHandle th = session.find(sha);
        if (th == null || !th.isValid()) {
            if (magnet == null) throw new IOException("torrent desconocido: falta el magnet");
            try {
                session.download(magnet, saveDir, TorrentFlags.SEQUENTIAL_DOWNLOAD);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (RuntimeException e) {
                lastError = "download: " + e.getMessage();
                throw new IOException("no se pudo añadir el magnet: " + e.getMessage());
            }
        }

        long deadline = System.currentTimeMillis() + resolveTimeoutMs;
        TorrentInfo ti = null;
        while (System.currentTimeMillis() < deadline) {
            th = session.find(sha);
            if (th != null && th.isValid()) {
                if (configured.putIfAbsent(hash, Boolean.TRUE) == null) {
                    // Sin cola automática: este torrent descarga ya, no espera a otro.
                    th.unsetFlags(TorrentFlags.AUTO_MANAGED);
                    th.resume();
                }
                if (th.status().hasMetadata()) {
                    ti = th.torrentFile();
                    if (ti != null && ti.isValid()) break;
                }
            }
            sleep(250);
        }
        if (ti == null) throw new TimeoutIOException("sin metadatos tras " + (resolveTimeoutMs / 1000) + " s (¿hay peers?)");

        FileStorage fsStorage = ti.files();
        int n = ti.numFiles();
        int idx = fileIndex;
        if (idx < 0) idx = pickFile(fsStorage, n);
        if (idx >= n) throw new IllegalArgumentException("el torrent solo tiene " + n + " archivos");

        // Solo se descarga el archivo elegido.
        String sel = hash + "#sel";
        if (configured.putIfAbsent(sel + idx, Boolean.TRUE) == null) {
            Priority[] pr = new Priority[n];
            for (int i = 0; i < n; i++) pr[i] = (i == idx) ? Priority.DEFAULT : Priority.IGNORE;
            th.prioritizeFiles(pr);
        }
        String path = fsStorage.filePath(idx);
        return new Meta(hash, idx, fsStorage.fileName(idx), fsStorage.fileSize(idx), mimeFor(path));
    }

    private static int pickFile(FileStorage fs, int n) {
        int best = -1, bestAny = 0;
        long bestSize = -1, bestAnySize = -1;
        for (int i = 0; i < n; i++) {
            long size = fs.fileSize(i);
            if (size > bestAnySize) {
                bestAnySize = size;
                bestAny = i;
            }
            if (isVideo(fs.filePath(i)) && size > bestSize) {
                bestSize = size;
                best = i;
            }
        }
        return best >= 0 ? best : bestAny;
    }

    // --------------------------------------------------------------------- read

    @Override
    public int read(Meta meta, long offset, byte[] buf, int off, int len) throws IOException {
        if (offset >= meta.size) return -1;
        if (!isRunning()) throw new IOException("el motor torrent no está en marcha");
        Sha1Hash sha = Sha1Hash.fromBytes(hexToBytes(meta.hash));
        TorrentHandle th = session.find(sha);
        if (th == null || !th.isValid()) throw new IOException("torrent no disponible");
        TorrentInfo ti = th.torrentFile();
        if (ti == null) throw new IOException("sin metadatos");

        int want = (int) Math.min(len, meta.size - offset);
        // Con un trozo a la vez: se lee lo que cubre el primer trozo pendiente.
        PeerRequest first = ti.mapFile(meta.fileIndex, offset, want);
        int piece = first.piece();
        // mapFile no recorta al límite del trozo: se limita la lectura al resto de este trozo.
        int restOfPiece = ti.files().pieceSize(piece) - first.start();
        int bytesInPiece = Math.min(want, restOfPiece);
        requestPieces(th, ti, piece);

        long deadline = System.currentTimeMillis() + pieceTimeoutMs;
        while (!th.havePiece(piece)) {
            if (!isRunning()) throw new IOException("el motor torrent se detuvo");
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutIOException("tiempo agotado esperando el trozo " + piece + " (peers: "
                        + th.status().numPeers() + ")");
            }
            sleep(40);
        }

        File f = new File(saveDir, ti.files().filePath(meta.fileIndex));
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            raf.seek(offset);
            int got = 0;
            while (got < bytesInPiece) {
                int r = raf.read(buf, off + got, bytesInPiece - got);
                if (r < 0) throw new IOException("archivo más corto de lo esperado");
                got += r;
            }
            return got;
        }
    }

    /** Prioridad máxima al trozo pedido y plazos escalonados a los siguientes (lectura anticipada). */
    private void requestPieces(TorrentHandle th, TorrentInfo ti, int piece) {
        int total = ti.numPieces();
        for (int i = 0; i < READAHEAD_PIECES && piece + i < total; i++) {
            int p = piece + i;
            if (!th.havePiece(p)) th.setPieceDeadline(p, 500 + i * 400);
        }
    }

    // ------------------------------------------------------------------- estado

    @Override
    public String statusJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"name\":\"libtorrent4j\",\"running\":").append(isRunning());
        if (isRunning()) {
            try {
                sb.append(",\"dhtNodes\":").append(session.dhtNodes());
                sb.append(",\"downloadRate\":").append(session.downloadRate());
                sb.append(",\"uploadRate\":").append(session.uploadRate());
                sb.append(",\"torrents\":[");
                boolean first = true;
                for (String h : configuredHashes()) {
                    TorrentHandle th = session.find(Sha1Hash.fromBytes(hexToBytes(h)));
                    if (th == null || !th.isValid()) continue;
                    TorrentStatus st = th.status();
                    if (!first) sb.append(',');
                    first = false;
                    sb.append("{\"hash\":").append(Json.str(h))
                            .append(",\"metadata\":").append(st.hasMetadata())
                            .append(",\"peers\":").append(st.numPeers())
                            .append(",\"progress\":").append(String.format(Locale.ROOT, "%.3f", st.progress()))
                            .append(",\"down\":").append(st.downloadPayloadRate()).append('}');
                }
                sb.append(']');
            } catch (RuntimeException e) {
                sb.append(",\"statusError\":").append(Json.str(String.valueOf(e.getMessage())));
            }
        }
        if (lastError != null) sb.append(",\"lastError\":").append(Json.str(lastError));
        return sb.append('}').toString();
    }

    private List<String> configuredHashes() {
        List<String> out = new ArrayList<>();
        for (String k : configured.keySet()) if (k.length() == 40) out.add(k);
        return out;
    }

    @Override
    public void close() {
        running = false;
        try {
            session.stop(); // bloquea mientras se destruye la sesión nativa
        } catch (RuntimeException e) {
            lastError = "stop: " + e.getMessage();
        }
        configured.clear();
    }

    // ------------------------------------------------------------------ utiles

    private static boolean isVideo(String path) {
        String p = path.toLowerCase(Locale.ROOT);
        for (String e : VIDEO_EXT) if (p.endsWith(e)) return true;
        return false;
    }

    static String mimeFor(String path) {
        String p = path.toLowerCase(Locale.ROOT);
        if (p.endsWith(".mp4") || p.endsWith(".m4v")) return "video/mp4";
        if (p.endsWith(".mkv")) return "video/x-matroska";
        if (p.endsWith(".webm")) return "video/webm";
        if (p.endsWith(".avi")) return "video/x-msvideo";
        if (p.endsWith(".mov")) return "video/quicktime";
        if (p.endsWith(".ts") || p.endsWith(".m2ts")) return "video/mp2t";
        if (p.endsWith(".mpg") || p.endsWith(".mpeg")) return "video/mpeg";
        if (p.endsWith(".wmv")) return "video/x-ms-wmv";
        return "application/octet-stream";
    }

    private static byte[] hexToBytes(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        return out;
    }

    private static void sleep(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrumpido");
        }
    }

    private static void deleteChildren(File dir) {
        File[] kids = dir.listFiles();
        if (kids == null) return;
        for (File k : kids) {
            if (k.isDirectory()) deleteChildren(k);
            k.delete();
        }
    }
}
