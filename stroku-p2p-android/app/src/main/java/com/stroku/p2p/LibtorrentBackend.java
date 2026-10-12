package com.stroku.p2p;

import org.libtorrent4j.FileStorage;
import org.libtorrent4j.Priority;
import org.libtorrent4j.SessionManager;
import org.libtorrent4j.Sha1Hash;
import org.libtorrent4j.TorrentHandle;
import org.libtorrent4j.TorrentInfo;
import org.libtorrent4j.TorrentStatus;
import org.libtorrent4j.swig.torrent_flags_t;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Real torrent engine: libtorrent4j (native libtorrent bundled inside the APK, no Termux, no
 * external process). Downloads into dataDir, selects only the chosen file, and steers piece
 * deadlines to the byte range the player is reading.
 *
 * This is the only class that touches libtorrent4j. It can only be exercised on an Android device.
 */
public final class LibtorrentBackend implements TorrentBackend {
    private static final String TAG = "torrent";
    private static final String[] VIDEO_EXT = {".mkv", ".mp4", ".m4v", ".avi", ".mov", ".webm", ".ts", ".mpg", ".mpeg", ".wmv"};
    private static final int MAX_WINDOW_PIECES = 160;

    private static final class Entry {
        final String hash;
        volatile TorrentHandle handle;
        volatile TorrentInfo info;
        volatile Meta meta;
        volatile long[] fileOffsets;
        volatile long lastUsed = System.nanoTime();
        int selectedFile = -1;
        int lastStartPiece = -1;
        boolean tailRequested;
        final Map<Integer, RandomAccessFile> rafs = new HashMap<>();

        Entry(String hash) {
            this.hash = hash;
        }
    }

    private final SessionManager sm = new SessionManager();
    private final File dataDir;
    private final int maxTorrents;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final Object addLock = new Object();

    public LibtorrentBackend(File dataDir, int maxTorrents) {
        this.dataDir = dataDir;
        this.maxTorrents = Math.max(1, maxTorrents);
    }

    public void start() throws IOException {
        if (!dataDir.isDirectory() && !dataDir.mkdirs()) throw new IOException("cannot create " + dataDir);
        wipeDataDir();
        try {
            sm.start();
        } catch (Throwable t) {
            throw new IOException("libtorrent4j failed to start: " + t, t);
        }
        if (!sm.isRunning()) throw new IOException("libtorrent4j session is not running after start()");
        Logx.i(TAG, "libtorrent4j session started, data dir " + dataDir);
    }

    // ------------------------------------------------------------------ adding

    @Override
    public Meta addMagnet(String sanitizedMagnet, long timeoutMs) throws IOException {
        final MagnetUtil.Parsed p;
        try {
            p = MagnetUtil.parse(sanitizedMagnet);
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
        Entry e;
        synchronized (addLock) {
            e = entries.get(p.infoHash);
            if (e == null) {
                makeRoomLocked();
                e = new Entry(p.infoHash);
                try {
                    sm.download(p.sanitized, dataDir, new torrent_flags_t());
                } catch (Throwable t) {
                    throw new IOException("could not add magnet: " + t, t);
                }
                entries.put(p.infoHash, e);
                Logx.i(TAG, "added torrent " + p.infoHash);
            }
        }
        e.lastUsed = System.nanoTime();
        if (e.meta != null) return e.meta;

        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        try {
            TorrentHandle h = null;
            while ((h = sm.find(new Sha1Hash(p.infoHash))) == null) {
                if (System.nanoTime() > deadline) throw new IOException("torrent was not registered by the engine in time");
                Thread.sleep(150);
            }
            while (!h.status().hasMetadata()) {
                if (System.nanoTime() > deadline) {
                    removeTorrent(p.infoHash);
                    throw new IOException("no metadata from peers/DHT within " + (timeoutMs / 1000) + " s");
                }
                Thread.sleep(250);
            }
            synchronized (e) {
                if (e.meta == null) {
                    TorrentInfo ti = h.torrentFile();
                    if (ti == null) throw new IOException("metadata reported but torrent info is null");
                    FileStorage fs = ti.files();
                    int n = fs.numFiles();
                    if (n <= 0) throw new IOException("torrent has no files");
                    List<FileMeta> files = new ArrayList<>(n);
                    long[] offsets = new long[n];
                    int best = 0;
                    long bestSize = -1;
                    boolean bestIsVideo = false;
                    for (int i = 0; i < n; i++) {
                        String path = fs.filePath(i);
                        long size = fs.fileSize(i);
                        offsets[i] = fs.fileOffset(i);
                        files.add(new FileMeta(i, path, size));
                        boolean video = isVideo(path);
                        if ((video && !bestIsVideo) || (video == bestIsVideo && size > bestSize)) {
                            best = i;
                            bestSize = size;
                            bestIsVideo = video;
                        }
                    }
                    e.handle = h;
                    e.info = ti;
                    e.fileOffsets = offsets;
                    e.meta = new Meta(p.infoHash, ti.name(), ti.pieceLength(), files, best);
                    Logx.i(TAG, "metadata ready: " + ti.name() + " (" + n + " files, playing #" + best + ")");
                }
            }
            return e.meta;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted");
        } catch (IOException ioe) {
            throw ioe;
        } catch (Throwable t) {
            throw new IOException("engine error: " + t, t);
        }
    }

    private static boolean isVideo(String path) {
        String n = path.toLowerCase(Locale.ROOT);
        for (String x : VIDEO_EXT) if (n.endsWith(x)) return true;
        return false;
    }

    private void makeRoomLocked() {
        while (entries.size() >= maxTorrents) {
            Entry oldest = null;
            for (Entry e : entries.values()) if (oldest == null || e.lastUsed < oldest.lastUsed) oldest = e;
            if (oldest == null) return;
            Logx.i(TAG, "torrent limit reached, removing " + oldest.hash);
            removeTorrent(oldest.hash);
        }
    }

    private void removeTorrent(String hash) {
        Entry e = entries.remove(hash);
        if (e == null) return;
        synchronized (e) {
            for (RandomAccessFile r : e.rafs.values()) {
                try {
                    r.close();
                } catch (IOException ignored) {
                }
            }
            e.rafs.clear();
        }
        try {
            if (e.handle != null) sm.remove(e.handle);
        } catch (Throwable t) {
            Logx.e(TAG, "remove failed for " + hash, t);
        }
        Meta m = e.meta;
        if (m != null && !m.files.isEmpty()) {
            String first = m.files.get(0).path.replace('\\', '/');
            int slash = first.indexOf('/');
            File root = new File(dataDir, slash > 0 ? first.substring(0, slash) : first);
            deleteInside(root);
        }
    }

    // ------------------------------------------------------------------ streaming support

    @Override
    public Meta meta(String infoHash) {
        Entry e = entries.get(infoHash);
        if (e == null) return null;
        e.lastUsed = System.nanoTime();
        return e.meta;
    }

    @Override
    public void prioritize(String infoHash, int fileIndex, long offset, long length) {
        Entry e = entries.get(infoHash);
        if (e == null || e.meta == null || length <= 0) return;
        try {
            e.lastUsed = System.nanoTime();
            TorrentHandle h = e.handle;
            TorrentInfo ti = e.info;
            synchronized (e) {
                selectFile(e, fileIndex);
                long pl = ti.pieceLength();
                int numPieces = ti.numPieces();
                long abs = e.fileOffsets[fileIndex] + offset;
                int first = (int) (abs / pl);
                int last = (int) Math.min((abs + length - 1) / pl, numPieces - 1L);
                last = Math.min(last, first + MAX_WINDOW_PIECES - 1);
                if (e.lastStartPiece >= 0 && (first < e.lastStartPiece || first > e.lastStartPiece + MAX_WINDOW_PIECES)) {
                    h.clearPieceDeadlines(); // the player jumped: forget the old read-ahead
                }
                e.lastStartPiece = first;
                int k = 0;
                for (int pc = first; pc <= last; pc++, k++) {
                    if (!h.havePiece(pc)) h.setPieceDeadline(pc, 80 + k * 120);
                }
                if (!e.tailRequested) { // MP4 files without "faststart" keep the index at the end
                    e.tailRequested = true;
                    long fileEnd = e.fileOffsets[fileIndex] + e.meta.files.get(fileIndex).size - 1;
                    int tailFirst = (int) Math.max(0, (fileEnd - 2 * pl) / pl);
                    int tailLast = (int) Math.min(fileEnd / pl, numPieces - 1L);
                    for (int pc = tailFirst; pc <= tailLast; pc++) {
                        if (!h.havePiece(pc)) h.setPieceDeadline(pc, 4000 + (pc - tailFirst) * 150);
                    }
                }
            }
        } catch (Throwable t) {
            Logx.e(TAG, "prioritize failed", t);
        }
    }

    private void selectFile(Entry e, int fileIndex) {
        if (e.selectedFile == fileIndex) return;
        int n = e.meta.files.size();
        Priority[] pr = new Priority[n];
        for (int i = 0; i < n; i++) pr[i] = i == fileIndex ? Priority.NORMAL : Priority.IGNORE;
        e.handle.prioritizeFiles(pr);
        e.selectedFile = fileIndex;
        e.tailRequested = false;
    }

    @Override
    public long availableFrom(String infoHash, int fileIndex, long offset, long maxLen) {
        Entry e = entries.get(infoHash);
        if (e == null || e.meta == null) return 0;
        try {
            TorrentHandle h = e.handle;
            long size = e.meta.files.get(fileIndex).size;
            long pl = e.meta.pieceLength;
            long base = e.fileOffsets[fileIndex];
            long pos = offset, n = 0;
            while (n < maxLen && pos < size) {
                long abs = base + pos;
                if (!h.havePiece((int) (abs / pl))) break;
                long chunk = Math.min(pl - (abs % pl), Math.min(maxLen - n, size - pos));
                n += chunk;
                pos += chunk;
            }
            return n;
        } catch (Throwable t) {
            Logx.e(TAG, "availableFrom failed", t);
            return 0;
        }
    }

    @Override
    public int read(String infoHash, int fileIndex, long offset, byte[] buf, int off, int len) throws IOException {
        Entry e = entries.get(infoHash);
        if (e == null || e.meta == null) throw new IOException("unknown torrent");
        RandomAccessFile raf;
        synchronized (e) {
            raf = e.rafs.get(fileIndex);
            if (raf == null) {
                File f = new File(dataDir, e.meta.files.get(fileIndex).path);
                String canon = f.getCanonicalPath();
                if (!canon.startsWith(dataDir.getCanonicalPath() + File.separator)) {
                    throw new IOException("file outside data dir refused");
                }
                raf = new RandomAccessFile(f, "r");
                e.rafs.put(fileIndex, raf);
            }
        }
        synchronized (raf) {
            raf.seek(offset);
            return raf.read(buf, off, len);
        }
    }

    // ------------------------------------------------------------------ status / lifecycle

    @Override
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("engine", "libtorrent4j");
        boolean running = false;
        try {
            running = sm.isRunning();
        } catch (Throwable ignored) {
        }
        m.put("running", running);
        long peers = 0, down = 0;
        List<Object> list = new ArrayList<>();
        for (Entry e : entries.values()) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("hash", e.hash);
            try {
                if (e.handle != null) {
                    TorrentStatus st = e.handle.status();
                    peers += st.numPeers();
                    down += st.downloadRate();
                    t.put("name", e.meta != null ? e.meta.name : "");
                    t.put("progress", Math.round(st.progress() * 1000) / 10.0);
                    t.put("peers", st.numPeers());
                } else {
                    t.put("state", "waiting for metadata");
                }
            } catch (Throwable ignored) {
            }
            list.add(t);
        }
        m.put("torrents", entries.size());
        m.put("peers", peers);
        m.put("downloadRate", down);
        m.put("list", list);
        return m;
    }

    @Override
    public void close() {
        for (Entry e : entries.values()) {
            synchronized (e) {
                for (RandomAccessFile r : e.rafs.values()) {
                    try {
                        r.close();
                    } catch (IOException ignored) {
                    }
                }
                e.rafs.clear();
            }
        }
        try {
            sm.stop();
        } catch (Throwable t) {
            Logx.e(TAG, "stop failed", t);
        }
        entries.clear();
        wipeDataDir();
        Logx.i(TAG, "libtorrent4j stopped, downloaded data removed");
    }

    private void wipeDataDir() {
        File[] fs = dataDir.listFiles();
        if (fs != null) for (File f : fs) deleteInside(f);
    }

    private void deleteInside(File f) {
        try {
            if (!f.getCanonicalPath().startsWith(dataDir.getCanonicalPath() + File.separator)) return;
        } catch (IOException e) {
            return;
        }
        File[] kids = f.listFiles();
        if (kids != null) for (File k : kids) deleteInside(k);
        f.delete();
    }
}
