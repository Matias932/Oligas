package com.stroku.p2p;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Internal torrent HTTP service on 127.0.0.1:3000 (loopback only, shared-secret header).
 * It is NOT KiraLT: it is this app's own engine API on top of the TorrentBackend.
 *
 *   GET /health                         -> JSON, no token
 *   GET /resolve?magnet=...             -> "hash \t fileIndex \t size \t name"  (token)
 *   GET|HEAD /stream/{hash}/{fileIndex} -> file bytes, Range supported (token)
 */
public final class EngineServer implements Closeable {
    private static final String TAG = "engine";
    private static final Pattern STREAM = Pattern.compile("^/stream/([0-9a-f]{40})/([0-9]{1,6})$");

    private final Config cfg;
    private final TorrentBackend backend;
    private MiniHttpServer server;

    public EngineServer(Config cfg, TorrentBackend backend) {
        this.cfg = cfg;
        this.backend = backend;
    }

    public void start() throws IOException {
        server = new MiniHttpServer("engine", cfg.maxConnections, cfg.engineIdleMs,
                addr -> addr.isLoopbackAddress(), this::handle);
        server.start(cfg.engineHost, cfg.enginePort);
        cfg.enginePort = server.getPort();
    }

    private void handle(MiniHttpServer.Request req, MiniHttpServer.Response res) throws Exception {
        if (req.path.equals("/health")) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", true);
            m.put("service", "stroku-engine");
            m.putAll(backend.status());
            res.text(200, "application/json", Json.write(m));
            return;
        }
        String tok = req.header("x-stroku-token");
        if (tok == null || !MessageDigest.isEqual(tok.getBytes(StandardCharsets.UTF_8),
                cfg.engineToken.getBytes(StandardCharsets.UTF_8))) {
            throw new MiniHttpServer.HttpError(403, "forbidden");
        }
        if (req.path.equals("/resolve")) {
            resolve(req, res);
            return;
        }
        Matcher m = STREAM.matcher(req.path);
        if (m.matches()) {
            stream(req, res, m.group(1), Integer.parseInt(m.group(2)));
            return;
        }
        throw new MiniHttpServer.HttpError(404, "not found");
    }

    private void resolve(MiniHttpServer.Request req, MiniHttpServer.Response res) throws Exception {
        MagnetUtil.Parsed p;
        try {
            p = MagnetUtil.parse(req.query.get("magnet"));
        } catch (IllegalArgumentException e) {
            throw new MiniHttpServer.HttpError(400, "invalid magnet: " + e.getMessage());
        }
        TorrentBackend.Meta meta;
        try {
            meta = backend.addMagnet(p.sanitized, cfg.metadataTimeoutMs);
        } catch (IOException e) {
            Logx.w(TAG, "resolve failed for " + p.infoHash + ": " + e.getMessage());
            throw new MiniHttpServer.HttpError(504, "metadata unavailable: " + e.getMessage());
        }
        TorrentBackend.FileMeta f = meta.files.get(meta.bestFile);
        String name = f.path.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
        res.text(200, "text/plain; charset=utf-8", meta.infoHash + "\t" + f.index + "\t" + f.size + "\t" + name + "\n");
    }

    private void stream(MiniHttpServer.Request req, MiniHttpServer.Response res, String hash, int idx) throws Exception {
        TorrentBackend.Meta meta = backend.meta(hash);
        if (meta == null || idx >= meta.files.size()) throw new MiniHttpServer.HttpError(404, "unknown torrent or file");
        long size = meta.files.get(idx).size;
        ByteRange r = ByteRange.parse(req.header("range"), size);
        long start = 0, end = size - 1;
        res.header("Accept-Ranges", "bytes").header("Content-Type", "application/octet-stream");
        if (r != null && r.isInvalid()) {
            res.status(416).header("Content-Range", "bytes */" + size);
            res.text(416, "text/plain", "range not satisfiable\n");
            return;
        }
        if (r != null) {
            start = r.start;
            end = r.end;
            res.status(206).header("Content-Range", "bytes " + start + "-" + end + "/" + size);
        }
        long total = size == 0 ? 0 : end - start + 1;
        try (OutputStream out = res.beginBody(total)) {
            if (req.isHead() || total == 0) return;
            long pos = start, remaining = total;
            final long window = cfg.engineWindowBytes;
            backend.prioritize(hash, idx, pos, Math.min(remaining, window));
            long lastPrio = pos;
            long lastWaitPrio = System.nanoTime();
            long waitedMs = 0;
            byte[] buf = new byte[64 * 1024];
            while (remaining > 0) {
                long want = Math.min(remaining, buf.length);
                long avail = backend.availableFrom(hash, idx, pos, want);
                if (avail <= 0) {
                    if (waitedMs >= cfg.pieceWaitTimeoutMs) {
                        throw new IOException("timed out waiting for pieces at offset " + pos);
                    }
                    if (System.nanoTime() - lastWaitPrio > 2_000_000_000L) {
                        backend.prioritize(hash, idx, pos, Math.min(remaining, window));
                        lastWaitPrio = System.nanoTime();
                    }
                    Thread.sleep(80);
                    waitedMs += 80;
                    continue;
                }
                waitedMs = 0;
                int n = backend.read(hash, idx, pos, buf, 0, (int) avail);
                if (n <= 0) throw new IOException("short read at offset " + pos);
                out.write(buf, 0, n);
                pos += n;
                remaining -= n;
                if (pos - lastPrio >= window / 2) {
                    backend.prioritize(hash, idx, pos, Math.min(remaining, window));
                    lastPrio = pos;
                }
            }
        }
    }

    public int port() {
        return server == null ? -1 : server.getPort();
    }

    @Override
    public void close() {
        if (server != null) server.close();
    }
}
