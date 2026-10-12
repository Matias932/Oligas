package com.stroku.p2p.core;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Único cliente de salida del gateway. El destino es SIEMPRE el motor local (loopback):
 * el host se valida al construir, la URL se arma aquí con un info-hash ya validado,
 * no se sigue ninguna redirección y se ignoran los proxies del sistema.
 */
public final class UpstreamClient {
    public static final class Info {
        public final long size;
        public final String mime;
        public final int file;

        Info(long size, String mime, int file) {
            this.size = size;
            this.mime = mime;
            this.file = file;
        }
    }

    private static final Pattern CONTENT_RANGE = Pattern.compile("bytes (\\d+)-(\\d+)/(\\d+)");

    private final String host;
    private final int port;
    private final int headTimeoutMs;
    private final int rangeTimeoutMs;

    public UpstreamClient(String host, int port, long resolveTimeoutMs, long pieceTimeoutMs) {
        try {
            if (!InetAddress.getByName(host).isLoopbackAddress()) {
                throw new IllegalArgumentException("el destino del gateway debe ser loopback: " + host);
            }
        } catch (java.net.UnknownHostException e) {
            throw new IllegalArgumentException("host inválido: " + host);
        }
        if (port < 1 || port > 65535) throw new IllegalArgumentException("puerto inválido: " + port);
        this.host = host;
        this.port = port;
        this.headTimeoutMs = (int) Math.min(Integer.MAX_VALUE, resolveTimeoutMs + 15_000);
        this.rangeTimeoutMs = (int) Math.min(Integer.MAX_VALUE, pieceTimeoutMs + 15_000);
    }

    private HttpURLConnection open(String pathAndQuery, int readTimeoutMs) throws IOException {
        URL url = new URL("http://" + host + ":" + port + pathAndQuery);
        if (!url.getHost().equals(host) || url.getPort() != port) throw new IOException("destino no autorizado");
        HttpURLConnection c = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
        c.setInstanceFollowRedirects(false);
        c.setConnectTimeout(5_000);
        c.setReadTimeout(readTimeoutMs);
        c.setUseCaches(false);
        return c;
    }

    private static String streamPath(String hash, int file, String magnet) throws IOException {
        if (Magnets.normalizeHash(hash) == null) throw new IOException("info-hash inválido");
        StringBuilder sb = new StringBuilder("/stream/").append(hash).append("?file=").append(file);
        if (magnet != null) sb.append("&magnet=").append(URLEncoder.encode(magnet, "UTF-8"));
        return sb.toString();
    }

    private static void checkStatus(HttpURLConnection c, int... ok) throws IOException {
        int code = c.getResponseCode();
        for (int o : ok) if (code == o) return;
        String detail = "";
        try {
            InputStream es = c.getErrorStream();
            if (es != null) {
                byte[] b = new byte[512];
                int n = es.read(b);
                if (n > 0) detail = new String(b, 0, n, "UTF-8");
                es.close();
            }
        } catch (IOException ignored) {
        }
        if (code == 504) throw new TorrentBackend.TimeoutIOException("el motor torrent no respondió a tiempo " + detail);
        if (code >= 300 && code < 400) throw new IOException("redirección rechazada (HTTP " + code + ")");
        throw new IOException("el motor respondió HTTP " + code + " " + detail);
    }

    /** Resuelve el torrent (añade el magnet si hace falta) y devuelve tamaño, tipo y archivo elegido. */
    public Info head(String hash, int file, String magnet) throws IOException {
        HttpURLConnection c = open(streamPath(hash, file, magnet), headTimeoutMs);
        try {
            c.setRequestMethod("HEAD");
            checkStatus(c, 200);
            long size = c.getContentLengthLong();
            if (size < 0) throw new IOException("el motor no indicó el tamaño");
            String mime = c.getContentType();
            int f = Integer.parseInt(c.getHeaderField("X-Stroku-File"));
            return new Info(size, mime == null ? "application/octet-stream" : mime, f);
        } catch (NumberFormatException e) {
            throw new IOException("respuesta del motor sin archivo");
        } finally {
            c.disconnect();
        }
    }

    /** Lee exactamente end-start+1 bytes. Cualquier respuesta distinta es un error: el bloque no se guarda. */
    public void fetchRange(String hash, int file, long start, long end, byte[] dst) throws IOException {
        int expected = (int) (end - start + 1);
        if (expected <= 0 || expected > dst.length) throw new IOException("rango inválido");
        HttpURLConnection c = open(streamPath(hash, file, null), rangeTimeoutMs);
        try {
            c.setRequestProperty("Range", "bytes=" + start + "-" + end);
            checkStatus(c, 206);
            String cr = c.getHeaderField("Content-Range");
            Matcher m = cr == null ? null : CONTENT_RANGE.matcher(cr);
            if (m == null || !m.matches() || Long.parseLong(m.group(1)) != start || Long.parseLong(m.group(2)) != end) {
                throw new IOException("Content-Range inesperado: " + cr);
            }
            if (c.getContentLengthLong() != expected) throw new IOException("Content-Length inesperado");
            InputStream in = c.getInputStream();
            int got = 0;
            while (got < expected) {
                int n = in.read(dst, got, expected - got);
                if (n < 0) throw new IOException("el motor cerró la conexión con " + got + "/" + expected + " bytes");
                got += n;
            }
            in.close();
        } finally {
            c.disconnect();
        }
    }
}
