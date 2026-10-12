package com.stroku.p2p;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * The gateway's only way out. The host and port come from Config (127.0.0.1 and the engine port),
 * paths are rebuilt from validated pieces, redirects are never followed, and nothing derived from a
 * client-supplied URL ever reaches the connection target.
 */
public final class EngineClient {
    public static final class NotFound extends IOException {
        public NotFound(String m) {
            super(m);
        }
    }

    public static final class Resolved {
        public final String hash;
        public final int fileIndex;
        public final long size;
        public final String name;

        Resolved(String hash, int fileIndex, long size, String name) {
            this.hash = hash;
            this.fileIndex = fileIndex;
            this.size = size;
            this.name = name;
        }
    }

    private final Config cfg;

    public EngineClient(Config cfg) {
        this.cfg = cfg;
    }

    private HttpURLConnection open(String pathAndQuery, int readTimeoutMs) throws IOException {
        if (!pathAndQuery.startsWith("/")) throw new IOException("bad internal path");
        URL u = new URL("http", cfg.engineHost, cfg.enginePort, pathAndQuery);
        HttpURLConnection c = (HttpURLConnection) u.openConnection();
        c.setInstanceFollowRedirects(false);
        c.setUseCaches(false);
        c.setConnectTimeout(3000);
        c.setReadTimeout(readTimeoutMs);
        c.setRequestProperty("X-Stroku-Token", cfg.engineToken);
        return c;
    }

    private static void ensureOk(HttpURLConnection c, int... okCodes) throws IOException {
        int code = c.getResponseCode();
        for (int ok : okCodes) if (code == ok) return;
        if (code >= 300 && code < 400) throw new IOException("engine tried to redirect (refused)");
        if (code == 404) throw new NotFound("engine: not found");
        throw new IOException("engine answered HTTP " + code);
    }

    public String health() throws IOException {
        HttpURLConnection c = open("/health", 3000);
        try {
            ensureOk(c, 200);
            return new String(readAll(c.getInputStream(), 64 * 1024), StandardCharsets.UTF_8);
        } finally {
            c.disconnect();
        }
    }

    public Resolved resolve(String sanitizedMagnet, String file, int fileIndex1) throws IOException {
        String path = "/resolve?magnet=" + URLEncoder.encode(sanitizedMagnet, "UTF-8");
        if (file != null && !file.isEmpty()) path += "&file=" + URLEncoder.encode(file, "UTF-8");
        if (fileIndex1 > 0) path += "&fileIndex=" + fileIndex1;
        HttpURLConnection c = open(path, (int) cfg.metadataTimeoutMs + 15_000);
        try {
            ensureOk(c, 200);
            String line = new String(readAll(c.getInputStream(), 8192), StandardCharsets.UTF_8).trim();
            String[] f = line.split("\t", 4);
            if (f.length < 4 || !MagnetUtil.isHash(f[0])) throw new IOException("bad resolve response");
            return new Resolved(f[0], Integer.parseInt(f[1]), Long.parseLong(f[2]), f[3]);
        } catch (NumberFormatException e) {
            throw new IOException("bad resolve response", e);
        } finally {
            c.disconnect();
        }
    }

    /** Downloads exactly [start, start+len) into dest; throws if the engine delivers anything else. */
    public void fetchRange(String hash, int fileIndex, long start, long len, File dest) throws IOException {
        if (!MagnetUtil.isHash(hash) || fileIndex < 0 || start < 0 || len <= 0) throw new IOException("bad range request");
        long end = start + len - 1;
        HttpURLConnection c = open("/stream/" + hash + "/" + fileIndex, (int) cfg.pieceWaitTimeoutMs + 15_000);
        c.setRequestProperty("Range", "bytes=" + start + "-" + end);
        try {
            ensureOk(c, 206);
            String cr = c.getHeaderField("Content-Range");
            if (cr == null || !cr.startsWith("bytes " + start + "-" + end + "/")) {
                throw new IOException("engine returned unexpected Content-Range: " + cr);
            }
            long got = 0;
            byte[] buf = new byte[64 * 1024];
            try (InputStream in = c.getInputStream();
                 OutputStream out = new BufferedOutputStream(new FileOutputStream(dest), 64 * 1024)) {
                int n;
                while (got < len && (n = in.read(buf, 0, (int) Math.min(buf.length, len - got))) >= 0) {
                    out.write(buf, 0, n);
                    got += n;
                }
            }
            if (got != len) throw new IOException("incomplete block from engine: " + got + "/" + len);
        } finally {
            c.disconnect();
        }
    }

    private static byte[] readAll(InputStream in, int max) throws IOException {
        try (InputStream i = in) {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            byte[] b = new byte[4096];
            int n;
            while ((n = i.read(b)) >= 0) {
                bo.write(b, 0, n);
                if (bo.size() > max) throw new IOException("response too large");
            }
            return bo.toByteArray();
        }
    }
}
