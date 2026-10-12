package com.stroku.p2p;

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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Small HTTP/1.1 server over plain sockets (com.sun.net.httpserver does not exist on Android).
 * GET and HEAD only, origin-form targets only, one request per connection, bounded header sizes,
 * client address filter, connection cap and an idle watchdog for stalled writers.
 */
public final class MiniHttpServer implements Closeable {
    private static final String TAG = "http";

    public interface Handler {
        void handle(Request req, Response res) throws Exception;
    }

    /** Thrown by handlers (or the parser) to produce a plain-text error response. */
    public static final class HttpError extends Exception {
        public final int status;

        public HttpError(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    public static final class Request {
        public final String method;
        public final String path;
        public final String rawQuery;
        public final Map<String, String> query;
        public final Map<String, String> headers; // lower-case names
        public final InetAddress remote;

        Request(String method, String path, String rawQuery, Map<String, String> query,
                Map<String, String> headers, InetAddress remote) {
            this.method = method;
            this.path = path;
            this.rawQuery = rawQuery;
            this.query = query;
            this.headers = headers;
            this.remote = remote;
        }

        public boolean isHead() {
            return "HEAD".equals(method);
        }

        public String header(String name) {
            return headers.get(name.toLowerCase(Locale.ROOT));
        }
    }

    private static final class Conn {
        final Socket socket;
        volatile long last = System.nanoTime();

        Conn(Socket s) {
            socket = s;
        }

        void touch() {
            last = System.nanoTime();
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    public static final class Response {
        private final OutputStream raw;
        private final boolean head;
        private final Conn conn;
        private int status = 200;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private boolean sent;

        Response(OutputStream raw, boolean head, Conn conn) {
            this.raw = raw;
            this.head = head;
            this.conn = conn;
        }

        public Response status(int s) {
            status = s;
            return this;
        }

        public Response header(String k, String v) {
            if (k.indexOf('\r') >= 0 || k.indexOf('\n') >= 0 || v.indexOf('\r') >= 0 || v.indexOf('\n') >= 0) {
                throw new IllegalArgumentException("header injection");
            }
            headers.put(k, v);
            return this;
        }

        public boolean headersSent() {
            return sent;
        }

        /** Sends the status line and headers; returns the body stream (discarding for HEAD). */
        public OutputStream beginBody(long length) throws IOException {
            if (sent) throw new IllegalStateException("headers already sent");
            sent = true;
            StringBuilder sb = new StringBuilder();
            sb.append("HTTP/1.1 ").append(status).append(' ').append(reason(status)).append("\r\n");
            for (Map.Entry<String, String> e : headers.entrySet()) {
                sb.append(e.getKey()).append(": ").append(e.getValue()).append("\r\n");
            }
            if (length >= 0) sb.append("Content-Length: ").append(length).append("\r\n");
            sb.append("Connection: close\r\n\r\n");
            raw.write(sb.toString().getBytes(StandardCharsets.ISO_8859_1));
            raw.flush();
            conn.touch();
            if (head) return new OutputStream() {
                @Override public void write(int b) {}
                @Override public void write(byte[] b, int o, int l) {}
            };
            return new OutputStream() {
                @Override public void write(int b) throws IOException {
                    raw.write(b);
                    conn.touch();
                }

                @Override public void write(byte[] b, int o, int l) throws IOException {
                    raw.write(b, o, l);
                    conn.touch();
                }

                @Override public void flush() throws IOException {
                    raw.flush();
                }
            };
        }

        public void text(int code, String contentType, String body) throws IOException {
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            status(code).header("Content-Type", contentType);
            try (OutputStream out = beginBody(b.length)) {
                out.write(b);
            }
        }
    }

    private final String name;
    private final int maxConns;
    private final long idleMs;
    private final Predicate<InetAddress> filter;
    private final Handler handler;
    private final Semaphore slots;
    private final Set<Conn> conns = ConcurrentHashMap.newKeySet();
    private ServerSocket ss;
    private ExecutorService pool;
    private ScheduledExecutorService watchdog;
    private volatile boolean closed;

    public MiniHttpServer(String name, int maxConns, long idleMs, Predicate<InetAddress> clientFilter, Handler handler) {
        this.name = name;
        this.maxConns = maxConns;
        this.idleMs = idleMs;
        this.filter = clientFilter;
        this.handler = handler;
        this.slots = new Semaphore(maxConns);
    }

    public void start(String host, int port) throws IOException {
        ServerSocket s = new ServerSocket();
        s.setReuseAddress(true);
        s.bind(new InetSocketAddress(InetAddress.getByName(host), port), 64);
        ss = s;
        pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, name + "-conn");
            t.setDaemon(true);
            return t;
        });
        watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, name + "-watchdog");
            t.setDaemon(true);
            return t;
        });
        watchdog.scheduleWithFixedDelay(this::reap, 5, 5, TimeUnit.SECONDS);
        Thread acc = new Thread(this::acceptLoop, name + "-accept");
        acc.setDaemon(true);
        acc.start();
        Logx.i(TAG, name + " listening on " + host + ":" + s.getLocalPort());
    }

    public int getPort() {
        return ss == null ? -1 : ss.getLocalPort();
    }

    public int activeConnections() {
        return conns.size();
    }

    private void reap() {
        long now = System.nanoTime();
        for (Conn c : conns) {
            if (now - c.last > idleMs * 1_000_000L) {
                Logx.w(TAG, name + ": closing idle connection");
                c.close();
            }
        }
    }

    private void acceptLoop() {
        while (!closed) {
            Socket s;
            try {
                s = ss.accept();
            } catch (IOException e) {
                if (closed) return;
                Logx.e(TAG, name + ": accept failed", e);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    return;
                }
                continue;
            }
            if (!filter.test(s.getInetAddress())) {
                quick(s, 403, "forbidden");
                continue;
            }
            if (!slots.tryAcquire()) {
                quick(s, 503, "busy");
                continue;
            }
            Conn c = new Conn(s);
            conns.add(c);
            try {
                pool.execute(() -> serve(c));
            } catch (RejectedExecutionException e) {
                conns.remove(c);
                slots.release();
                c.close();
            }
        }
    }

    private void quick(Socket s, int code, String msg) {
        try {
            s.setSoTimeout(2000);
            byte[] b = (msg + "\n").getBytes(StandardCharsets.UTF_8);
            String h = "HTTP/1.1 " + code + " " + reason(code) + "\r\nContent-Type: text/plain\r\nContent-Length: "
                    + b.length + "\r\nConnection: close\r\n\r\n";
            OutputStream o = s.getOutputStream();
            o.write(h.getBytes(StandardCharsets.ISO_8859_1));
            o.write(b);
            o.flush();
        } catch (IOException ignored) {
        } finally {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void serve(Conn c) {
        Socket s = c.socket;
        try {
            s.setSoTimeout(15_000);
            s.setTcpNoDelay(true);
            InputStream in = new BufferedInputStream(s.getInputStream(), 4096);
            OutputStream out = s.getOutputStream();
            Request req;
            try {
                req = readRequest(in, s.getInetAddress());
            } catch (HttpError e) {
                errorReply(out, c, e);
                return;
            }
            if (req == null) return;
            c.touch();
            Response res = new Response(out, req.isHead(), c);
            try {
                handler.handle(req, res);
            } catch (HttpError e) {
                if (!res.sent) errorReply(out, c, e);
            } catch (Throwable t) {
                if (!res.sent) {
                    Logx.e(TAG, name + ": handler error on " + req.path, t);
                    try {
                        res.text(500, "text/plain; charset=utf-8", "internal error\n");
                    } catch (IOException ignored) {
                    }
                } else {
                    Logx.i(TAG, name + ": stream ended early on " + req.path + " (" + t.getClass().getSimpleName()
                            + ": " + t.getMessage() + ")");
                }
            }
        } catch (IOException e) {
            // client went away: normal for players that seek
        } finally {
            conns.remove(c);
            c.close();
            slots.release();
        }
    }

    private void errorReply(OutputStream out, Conn c, HttpError e) {
        try {
            Response r = new Response(out, false, c);
            r.text(e.status, "text/plain; charset=utf-8", e.getMessage() + "\n");
        } catch (IOException ignored) {
        }
    }

    private static Request readRequest(InputStream in, InetAddress remote) throws IOException, HttpError {
        String line = readLine(in, 8192);
        if (line == null) return null;
        String[] parts = line.split(" ");
        if (parts.length != 3 || !parts[2].startsWith("HTTP/1.")) throw new HttpError(400, "bad request line");
        String method = parts[0].toUpperCase(Locale.ROOT);
        if (!method.equals("GET") && !method.equals("HEAD")) throw new HttpError(405, "method not allowed");
        String target = parts[1];
        if (!target.startsWith("/")) throw new HttpError(400, "only origin-form request targets are accepted");
        Map<String, String> headers = new LinkedHashMap<>();
        int total = 0;
        for (int n = 0; ; n++) {
            String h = readLine(in, 8192);
            if (h == null) throw new HttpError(400, "truncated headers");
            if (h.isEmpty()) break;
            total += h.length();
            if (n >= 64 || total > 16384) throw new HttpError(431, "headers too large");
            int colon = h.indexOf(':');
            if (colon <= 0) throw new HttpError(400, "bad header");
            String k = h.substring(0, colon).trim().toLowerCase(Locale.ROOT);
            if (!headers.containsKey(k)) headers.put(k, h.substring(colon + 1).trim());
        }
        int q = target.indexOf('?');
        String path = q >= 0 ? target.substring(0, q) : target;
        String rawQuery = q >= 0 ? target.substring(q + 1) : "";
        Map<String, String> query = new LinkedHashMap<>();
        if (!rawQuery.isEmpty()) {
            for (String kv : rawQuery.split("&")) {
                int eq = kv.indexOf('=');
                String k = eq >= 0 ? kv.substring(0, eq) : kv;
                String v = eq >= 0 ? kv.substring(eq + 1) : "";
                try {
                    k = URLDecoder.decode(k, "UTF-8");
                    v = URLDecoder.decode(v, "UTF-8");
                } catch (IllegalArgumentException ex) {
                    throw new HttpError(400, "bad query encoding");
                }
                if (!query.containsKey(k)) query.put(k, v);
            }
        }
        return new Request(method, path, rawQuery, query, headers, remote);
    }

    private static String readLine(InputStream in, int max) throws IOException, HttpError {
        ByteArrayOutputStream bo = new ByteArrayOutputStream(128);
        int c;
        boolean any = false;
        while ((c = in.read()) >= 0) {
            any = true;
            if (c == '\n') {
                byte[] b = bo.toByteArray();
                int len = b.length;
                if (len > 0 && b[len - 1] == '\r') len--;
                return new String(b, 0, len, StandardCharsets.ISO_8859_1);
            }
            bo.write(c);
            if (bo.size() > max) throw new HttpError(414, "line too long");
        }
        return null; // EOF before a complete line
    }

    static String reason(int code) {
        switch (code) {
            case 200: return "OK";
            case 206: return "Partial Content";
            case 400: return "Bad Request";
            case 403: return "Forbidden";
            case 404: return "Not Found";
            case 405: return "Method Not Allowed";
            case 414: return "URI Too Long";
            case 416: return "Range Not Satisfiable";
            case 431: return "Request Header Fields Too Large";
            case 500: return "Internal Server Error";
            case 502: return "Bad Gateway";
            case 503: return "Service Unavailable";
            case 504: return "Gateway Timeout";
            default: return "Status";
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (ss != null) ss.close();
        } catch (IOException ignored) {
        }
        for (Conn c : conns) c.close();
        if (watchdog != null) watchdog.shutdownNow();
        if (pool != null) pool.shutdownNow();
        Logx.i(TAG, name + " stopped");
    }
}
