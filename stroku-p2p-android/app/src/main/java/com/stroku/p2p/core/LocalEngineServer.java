package com.stroku.p2p.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * API HTTP local del motor torrent (puerto 3000, SOLO 127.0.0.1).
 *
 *   GET|HEAD /health
 *   GET|HEAD /stream/&lt;infohash40hex&gt;?file=N&amp;magnet=&lt;magnet&gt;   (Range soportado)
 *
 * Este servicio es propio de esta aplicación: no es KiraLT.
 */
public final class LocalEngineServer implements Closeable {
    private final Config cfg;
    private final TorrentBackend backend;
    private final MiniHttp http;

    public LocalEngineServer(Config cfg, TorrentBackend backend) {
        this.cfg = cfg;
        this.backend = backend;
        this.http = new MiniHttp("engine", Config.ENGINE_HOST, cfg.enginePort, 16, new MiniHttp.Handler() {
            @Override
            public void handle(MiniHttp.Request req, MiniHttp.Response res) throws Exception {
                route(req, res);
            }
        });
    }

    public void start() throws IOException {
        http.start();
    }

    public int getPort() {
        return http.getPort();
    }

    private void route(MiniHttp.Request req, MiniHttp.Response res) throws IOException {
        if (req.path.equals("/health")) {
            res.json(200, "{\"ok\":true,\"engine\":" + backend.statusJson() + "}");
            return;
        }
        if (req.path.startsWith("/stream/")) {
            stream(req, res);
            return;
        }
        res.error(404, "no encontrado");
    }

    private void stream(MiniHttp.Request req, MiniHttp.Response res) throws IOException {
        String hash = Magnets.normalizeHash(req.path.substring("/stream/".length()));
        if (hash == null) {
            res.error(400, "info-hash inválido");
            return;
        }
        String magnet = null;
        String m = req.query.get("magnet");
        if (m != null) {
            Magnets.Parsed p = Magnets.parse(m);
            if (p == null || !p.infoHash.equals(hash)) {
                res.error(400, "magnet inválido o distinto del info-hash");
                return;
            }
            magnet = p.magnet;
        }
        int file = -1;
        String f = req.query.get("file");
        if (f != null) {
            try {
                file = Integer.parseInt(f);
            } catch (NumberFormatException e) {
                res.error(400, "file inválido");
                return;
            }
            if (file < -1 || file > 100000) {
                res.error(400, "file fuera de rango");
                return;
            }
        }

        TorrentBackend.Meta meta;
        try {
            meta = backend.resolve(hash, magnet, file);
        } catch (TorrentBackend.TimeoutIOException e) {
            res.error(504, e.getMessage());
            return;
        } catch (IllegalArgumentException e) {
            res.error(400, e.getMessage());
            return;
        } catch (IOException e) {
            res.error(502, e.getMessage() == null ? "error del motor torrent" : e.getMessage());
            return;
        }

        long[] range;
        try {
            range = Ranges.parse(req.headers.get("range"), meta.size);
        } catch (Ranges.Unsatisfiable e) {
            Map<String, String> h = new LinkedHashMap<>();
            h.put("Content-Range", "bytes */" + meta.size);
            res.start(416, 0, "text/plain", h);
            return;
        }
        boolean partial = range != null;
        long start = partial ? range[0] : 0;
        long end = partial ? range[1] : meta.size - 1;

        Map<String, String> h = new LinkedHashMap<>();
        h.put("X-Stroku-File", String.valueOf(meta.fileIndex));
        if (partial) h.put("Content-Range", "bytes " + start + "-" + end + "/" + meta.size);
        long length = meta.size == 0 ? 0 : end - start + 1;
        res.start(partial ? 206 : 200, length, meta.mime, h);
        if (req.isHead() || length == 0) return;

        byte[] buf = new byte[64 * 1024];
        long pos = start;
        while (pos <= end) {
            int want = (int) Math.min(buf.length, end - pos + 1);
            int n = backend.read(meta, pos, buf, 0, want);
            if (n <= 0) throw new IOException("fin de archivo inesperado en " + pos);
            res.write(buf, 0, n);
            pos += n;
        }
    }

    @Override
    public void close() {
        http.close();
    }
}
