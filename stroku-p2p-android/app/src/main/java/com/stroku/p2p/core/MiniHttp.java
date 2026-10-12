package com.stroku.p2p.core;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Servidor HTTP/1.1 mínimo (solo GET y HEAD, una petición por conexión, sin proxy).
 * Rechaza cualquier URI que no sea de la forma "/ruta" (por ejemplo "http://host/...").
 */
public final class MiniHttp implements Closeable {

    public interface Handler {
        void handle(Request req, Response res) throws Exception;
    }

    public static final class Request {
        public final String method;
        public final String path;
        public final Map<String, String> query;
        public final Map<String, String> headers; // nombres en minúsculas
        public final String remote;

        Request(String method, String path, Map<String, String> query, Map<String, String> headers, String remote) {
            this.method = method;
            this.path = path;
            this.query = query;
            this.headers = headers;
            this.remote = remote;
        }

        public boolean isHead() {
            return method.equals("HEAD");
        }
    }

    public static final class Response {
        private final OutputStream out;
        private final boolean head;
        private boolean started;

        Response(OutputStream out, boolean head) {
            this.out = out;
            this.head = head;
        }

        public boolean isStarted() {
            return started;
        }

        /** Envía la línea de estado y las cabeceras. contentLength < 0 = sin Content-Length. */
        public void start(int status, long contentLength, String contentType, Map<String, String> extra)
                throws IOException {
            if (started) throw new IllegalStateException("cabeceras ya enviadas");
            started = true;
            StringBuilder sb = new StringBuilder(256);
            sb.append("HTTP/1.1 ").append(status).append(' ').append(reason(status)).append("\r\n");
            if (contentType != null) sb.append("Content-Type: ").append(contentType).append("\r\n");
            if (contentLength >= 0) sb.append("Content-Length: ").append(contentLength).append("\r\n");
            sb.append("Accept-Ranges: bytes\r\n");
            sb.append("Cache-Control: no-store\r\n");
            sb.append("Connection: close\r\n");
            if (extra != null) {
                for (Map.Entry<String, String> e : extra.entrySet()) {
                    String k = e.getKey(), v = e.getValue();
                    if (k.indexOf('\r') >= 0 || k.indexOf('\n') >= 0 || v.indexOf('\r') >= 0 || v.indexOf('\n') >= 0) {
                        continue; // evita inyección de cabeceras
                    }
                    sb.append(k).append(": ").append(v).append("\r\n");
                }
            }
            sb.append("\r\n");
            out.write(sb.toString().getBytes(StandardCharsets.ISO_8859_1));
        }

        /** Escribe cuerpo; en HEAD se ignora. */
        public void write(byte[] b, int off, int len) throws IOException {
            if (head) return;
            out.write(b, off, len);
        }

        public void json(int status, String body) throws IOException {
            byte[] data = body.getBytes(StandardCharsets.UTF_8);
            start(status, data.length, "application/json; charset=utf-8", null);
            write(data, 0, data.length);
        }

        public void error(int status, String message) throws IOException {
            if (started) return;
            json(status, "{\"ok\":false,\"error\":" + Json.str(message) + "}");
        }

        void flush() throws IOException {
            out.flush();
        }
    }

    static String reason(int status) {
        switch (status) {
            case 200: return "OK";
            case 206: return "Partial Content";
            case 400: return "Bad Request";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 416: return "Range Not Satisfiable";
            case 500: return "Internal Server Error";
            case 502: return "Bad Gateway";
            case 503: return "Service Unavailable";
            case 504: return "Gateway Timeout";
            default: return "Status";
        }
    }

    private static final int MAX_LINE = 8192;
    private static final int MAX_HEADERS = 64;

    private final String name;
    private final String bindHost;
    private final int requestedPort;
    private final Handler handler;
    private final Semaphore slots;
    private ServerSocket server;
    private ExecutorService pool;
    private Thread acceptor;
    private volatile boolean closed;

    public MiniHttp(String name, String bindHost, int port, int maxConnections, Handler handler) {
        this.name = name;
        this.bindHost = bindHost;
        this.requestedPort = port;
        this.handler = handler;
        this.slots = new Semaphore(maxConnections);
    }

    /** @throws IOException por ejemplo BindException si el puerto ya está ocupado. */
    public void start() throws IOException {
        ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(InetAddress.getByName(bindHost), requestedPort), 64);
        server = ss;
        final AtomicInteger n = new AtomicInteger();
        pool = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, name + "-conn-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        acceptor = new Thread(new Runnable() {
            @Override
            public void run() {
                acceptLoop();
            }
        }, name + "-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public int getPort() {
        return server == null ? -1 : server.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            final Socket s;
            try {
                s = server.accept();
            } catch (IOException e) {
                if (closed) return;
                continue;
            }
            if (!slots.tryAcquire()) {
                rejectBusy(s);
                continue;
            }
            try {
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            serve(s);
                        } finally {
                            slots.release();
                        }
                    }
                });
            } catch (RuntimeException e) { // pool cerrado
                slots.release();
                closeQuietly(s);
            }
        }
    }

    private void rejectBusy(Socket s) {
        try {
            byte[] body = "{\"ok\":false,\"error\":\"demasiadas conexiones\"}".getBytes(StandardCharsets.UTF_8);
            OutputStream o = s.getOutputStream();
            o.write(("HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: "
                    + body.length + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
            o.write(body);
            o.flush();
        } catch (IOException ignored) {
        } finally {
            closeQuietly(s);
        }
    }

    private void serve(Socket s) {
        try {
            s.setSoTimeout(20_000);
            s.setTcpNoDelay(true);
            InputStream in = new BufferedInputStream(s.getInputStream(), 4096);
            OutputStream out = s.getOutputStream();
            Request req;
            try {
                req = readRequest(in, s);
            } catch (BadRequest e) {
                Response r = new Response(out, false);
                r.error(e.status, e.getMessage());
                r.flush();
                return;
            }
            s.setSoTimeout(0);
            Response res = new Response(out, req.isHead());
            try {
                handler.handle(req, res);
            } catch (Throwable t) {
                try {
                    if (!res.isStarted()) {
                        String m = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
                        res.error(500, m);
                    }
                } catch (IOException ignored) {
                }
            }
            res.flush();
        } catch (IOException ignored) {
            // el cliente cerró o expiró: normal en streaming
        } finally {
            closeQuietly(s);
        }
    }

    private static final class BadRequest extends Exception {
        final int status;

        BadRequest(int status, String m) {
            super(m);
            this.status = status;
        }
    }

    private Request readRequest(InputStream in, Socket s) throws IOException, BadRequest {
        String line = readLine(in);
        if (line == null || line.isEmpty()) throw new BadRequest(400, "petición vacía");
        String[] parts = line.split(" ");
        if (parts.length != 3 || !parts[2].startsWith("HTTP/1.")) throw new BadRequest(400, "línea de petición inválida");
        String method = parts[0];
        if (!method.equals("GET") && !method.equals("HEAD")) throw new BadRequest(405, "método no permitido");
        String target = parts[1];
        // Solo forma de origen "/ruta". Rechaza "http://...", "host:puerto" (CONNECT) y "*".
        if (!target.startsWith("/") || target.startsWith("//")) throw new BadRequest(400, "URI no permitida");
        String rawPath = target;
        String rawQuery = "";
        int q = target.indexOf('?');
        if (q >= 0) {
            rawPath = target.substring(0, q);
            rawQuery = target.substring(q + 1);
        }
        if (rawPath.length() > 512 || rawPath.indexOf('%') >= 0 || rawPath.indexOf('\\') >= 0
                || rawPath.contains("..")) {
            throw new BadRequest(400, "ruta inválida");
        }
        Map<String, String> query = new HashMap<>();
        if (!rawQuery.isEmpty()) {
            if (rawQuery.length() > 6000) throw new BadRequest(400, "consulta demasiado larga");
            for (String kv : rawQuery.split("&")) {
                if (kv.isEmpty()) continue;
                int eq = kv.indexOf('=');
                String k = eq < 0 ? kv : kv.substring(0, eq);
                String v = eq < 0 ? "" : kv.substring(eq + 1);
                try {
                    query.put(URLDecoder.decode(k, "UTF-8"), URLDecoder.decode(v, "UTF-8"));
                } catch (IllegalArgumentException e) {
                    throw new BadRequest(400, "codificación de consulta inválida");
                }
            }
        }
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; ; i++) {
            if (i > MAX_HEADERS) throw new BadRequest(400, "demasiadas cabeceras");
            String h = readLine(in);
            if (h == null) throw new BadRequest(400, "cabeceras incompletas");
            if (h.isEmpty()) break;
            int c = h.indexOf(':');
            if (c <= 0) throw new BadRequest(400, "cabecera inválida");
            headers.put(h.substring(0, c).trim().toLowerCase(Locale.ROOT), h.substring(c + 1).trim());
        }
        String cl = headers.get("content-length");
        if ((cl != null && !cl.equals("0")) || headers.containsKey("transfer-encoding")) {
            throw new BadRequest(400, "no se admite cuerpo en la petición");
        }
        String remote = String.valueOf(s.getRemoteSocketAddress());
        return new Request(method, rawPath, query, headers, remote);
    }

    private static String readLine(InputStream in) throws IOException, BadRequest {
        ByteArrayOutputStream bo = new ByteArrayOutputStream(128);
        int prev = -1;
        while (true) {
            int b = in.read();
            if (b < 0) return bo.size() == 0 ? null : bo.toString("ISO-8859-1");
            if (b == '\n') {
                byte[] a = bo.toByteArray();
                int len = a.length;
                if (len > 0 && a[len - 1] == '\r') len--;
                return new String(a, 0, len, StandardCharsets.ISO_8859_1);
            }
            if (bo.size() >= MAX_LINE) throw new BadRequest(400, "línea demasiado larga");
            bo.write(b);
            prev = b;
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (server != null) server.close();
        } catch (IOException ignored) {
        }
        if (pool != null) pool.shutdownNow();
    }
}
