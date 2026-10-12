package com.stroku.p2p;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LAN-facing HTTP Range gateway (0.0.0.0:4001).
 *
 *   GET|HEAD /play?magnet=<magnet URI>   -> the movie file, 200 / 206 with Range
 *   GET      /health                     -> JSON status
 *
 * Data is pulled from the engine in fixed-size blocks, cached on disk, prefetched ahead of the
 * player and re-prioritised after a seek. The gateway can only ever talk to the engine.
 */
public final class Gateway implements Closeable {
    private static final String TAG = "gateway";

    private static final class Ref {
        final String hash;
        final int fileIndex;
        final long size;
        final String name;

        Ref(EngineClient.Resolved r) {
            hash = r.hash;
            fileIndex = r.fileIndex;
            size = r.size;
            name = r.name;
        }

        String key() {
            return hash + "-" + fileIndex;
        }

        int blocks(int bs) {
            return (int) ((size + bs - 1) / bs);
        }
    }

    private static final class Flow {
        volatile int lastBlock = -100;
        volatile int run;
        final Set<BlockTask> pending = ConcurrentHashMap.newKeySet();
    }

    private final Config cfg;
    private final EngineClient engine;
    private final BlockCache cache;
    private final ConcurrentHashMap<String, CompletableFuture<Ref>> refs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockTask> inflight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Flow> flows = new ConcurrentHashMap<>();
    private ExecutorService demandPool;
    private ExecutorService prefetchPool;
    private MiniHttpServer server;
    private volatile boolean closed;

    public Gateway(Config cfg, BlockCache cache) {
        this.cfg = cfg;
        this.cache = cache;
        this.engine = new EngineClient(cfg);
    }

    public void start() throws IOException {
        demandPool = Executors.newFixedThreadPool(6, r -> daemon(r, "gw-demand"));
        prefetchPool = Executors.newFixedThreadPool(Math.max(1, cfg.parallelFetches), r -> daemon(r, "gw-prefetch"));
        server = new MiniHttpServer("gateway", cfg.maxConnections, cfg.gatewayIdleMs, Gateway::lanClient, this::handle);
        java.io.IOException last = null;
        for (int i = 0; i < 4; i++) { // a just-closed socket from a previous run can need a moment
            try {
                server.start(cfg.gatewayBind, cfg.gatewayPort);
                last = null;
                break;
            } catch (java.net.BindException e) {
                last = e;
                Logx.w("gateway", "bind " + cfg.gatewayPort + " failed (try " + (i + 1) + "/4): " + e.getMessage());
                try {
                    Thread.sleep(700);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (last != null) {
            throw new java.io.IOException("El puerto " + cfg.gatewayPort + " (gateway) está ocupado por otro programa. Quien responde ahí: ["
                    + NetUtil.whoIsOn(cfg.gatewayPort) + "]. Ciérralo (Termux, otro servidor, versión vieja) y reintenta. ", last);
        }
        cfg.gatewayPort = server.getPort();
    }

    private static Thread daemon(Runnable r, String n) {
        Thread t = new Thread(r, n);
        t.setDaemon(true);
        return t;
    }

    /** Only loopback and private/link-local/ULA clients may use the gateway. */
    static boolean lanClient(InetAddress a) {
        if (a.isLoopbackAddress() || a.isSiteLocalAddress() || a.isLinkLocalAddress()) return true;
        byte[] b = a.getAddress();
        return b.length == 16 && (b[0] & 0xfe) == 0xfc; // fc00::/7
    }

    public int port() {
        return server == null ? -1 : server.getPort();
    }

    // ------------------------------------------------------------------ routing

    private void handle(MiniHttpServer.Request req, MiniHttpServer.Response res) throws Exception {
        switch (req.path) {
            case "/health":
                health(res);
                return;
            case "/play":      // simple form: /play?magnet=...
            case "/stream":    // Stroku's contract: /stream?url=http://127.0.0.1:3000/stream/<magnet>?file=...
                play(req, res);
                return;
            case "/probe":
                probe(req, res);
                return;
            case "/warm":
                warm(req, res);
                return;
            case "/":
                res.text(200, "text/plain; charset=utf-8", "Stroku P2P gateway. Use /stream?url=..., /probe, /warm, /play?magnet=... or /health\n");
                return;
            default:
                throw new MiniHttpServer.HttpError(404, "not found");
        }
    }

    private void health(MiniHttpServer.Response res) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", true);
        m.put("service", "stroku-gateway");
        m.put("port", port());
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("bytes", cache.totalBytes());
        c.put("max", cache.maxBytes());
        c.put("blocks", cache.blockCount());
        m.put("cache", c);
        m.put("connections", server.activeConnections());
        String eng;
        boolean engOk;
        try {
            eng = engine.health();
            engOk = eng.trim().startsWith("{");
            if (!engOk) eng = "null";
        } catch (IOException e) {
            engOk = false;
            eng = "null";
            m.put("engineError", String.valueOf(e.getMessage()));
        }
        m.put("engineOk", engOk);
        String json = Json.write(m);
        if (engOk) json = json.substring(0, json.length() - 1) + ",\"engine\":" + eng.trim() + "}";
        res.text(200, "application/json", json);
    }

    private static String magnetParam(MiniHttpServer.Request req) {
        // Accept both a properly encoded value and an un-encoded magnet whose own '&' would
        // otherwise split the query (common in hand-built BrightScript URLs).
        String rq = req.rawQuery;
        if (rq.startsWith("magnet=")) {
            String v = rq.substring(7);
            if (v.regionMatches(true, 0, "magnet:", 0, 7)) return v;
            try {
                return URLDecoder.decode(v, "UTF-8");
            } catch (Exception e) {
                return null;
            }
        }
        return req.query.get("magnet");
    }

    // ------------------------------------------------------------------ /play

    private void play(MiniHttpServer.Request req, MiniHttpServer.Response res) throws Exception {
        final Sel sel = selection(req);
        final int bs = cfg.blockSize;
        Ref ref = null;
        BlockCache.Lease first = null;
        ByteRange range = null;
        for (int attempt = 0; attempt < 2 && first == null; attempt++) {
            try {
                ref = resolve(sel);
                range = ByteRange.parse(req.header("range"), ref.size);
                if (range != null && range.isInvalid()) break;
                long start = range == null ? 0 : range.start;
                if (!req.isHead() && ref.size > 0) first = getBlock(ref, (int) (start / bs));
                else break;
            } catch (EngineClient.NotFound nf) {
                refs.remove(sel.key()); // engine dropped the torrent: resolve again
                if (attempt == 1) throw new MiniHttpServer.HttpError(502, "torrent no longer available");
            } catch (IOException e) {
                throw new MiniHttpServer.HttpError(e instanceof TimeoutLike ? 504 : 502, "upstream failure: " + e.getMessage());
            }
        }
        try {
            if (ref == null) throw new MiniHttpServer.HttpError(502, "torrent unavailable");
            long size = ref.size;
            res.header("Accept-Ranges", "bytes").header("Content-Type", contentType(ref.name));
            if (range != null && range.isInvalid()) {
                res.header("Content-Range", "bytes */" + size);
                res.text(416, "text/plain", "range not satisfiable\n");
                return;
            }
            long start = 0, end = size - 1;
            if (range != null) {
                start = range.start;
                end = range.end;
                res.status(206).header("Content-Range", "bytes " + start + "-" + end + "/" + size);
            }
            long total = size == 0 ? 0 : end - start + 1;
            try (OutputStream out = res.beginBody(total)) {
                if (req.isHead() || total == 0) return;
                streamBlocks(ref, start, end, out, first);
                first = null; // ownership passed
            }
        } finally {
            if (first != null) first.close();
        }
    }

    /** Marker for upstream timeouts so they map to 504. */
    private static final class TimeoutLike extends IOException {
        TimeoutLike(String m) {
            super(m);
        }
    }

    private Ref resolve(Sel sel) throws IOException {
        final String k = sel.key();
        CompletableFuture<Ref> f = new CompletableFuture<>();
        CompletableFuture<Ref> existing = refs.putIfAbsent(k, f);
        if (existing == null) {
            try {
                f.complete(new Ref(engine.resolve(sel.parsed.sanitized, sel.file, sel.fileIndex)));
            } catch (Throwable t) {
                refs.remove(k, f);
                f.completeExceptionally(t);
            }
            existing = f;
        }
        try {
            return existing.get(cfg.metadataTimeoutMs + 20_000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            refs.remove(k, existing);
            Throwable c = e.getCause();
            if (c instanceof IOException) throw (IOException) c;
            throw new IOException(String.valueOf(c), c);
        } catch (TimeoutException e) {
            throw new TimeoutLike("metadata timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted");
        }
    }

    // ------------------------------------------------------------------ request selection

    /** What the client wants: a magnet plus an optional file selector (name or one-based index). */
    private static final class Sel {
        final MagnetUtil.Parsed parsed;
        final String file;     // file name hint or null
        final int fileIndex;   // one-based, 0 = none

        Sel(MagnetUtil.Parsed parsed, String file, int fileIndex) {
            this.parsed = parsed;
            this.file = file;
            this.fileIndex = fileIndex;
        }

        String key() {
            return parsed.infoHash + "|" + (file == null ? "" : file) + "|" + fileIndex;
        }
    }

    private Sel selection(MiniHttpServer.Request req) throws MiniHttpServer.HttpError {
        if (req.path.equals("/play")) {
            try {
                return new Sel(MagnetUtil.parse(magnetParam(req)), null, 0);
            } catch (IllegalArgumentException e) {
                throw new MiniHttpServer.HttpError(400, "invalid magnet: " + e.getMessage());
            }
        }
        String url = req.query.get("url");
        if (url == null || url.isEmpty()) throw new MiniHttpServer.HttpError(400, "Missing ?url=");
        return parseTarget(url);
    }

    /**
     * Accepts only what Stroku sends: http(s)://127.0.0.1:<engine port>/stream/<encoded magnet>
     * with an optional ?file=<name> or ?fileIndex=<one-based>. Anything else is refused, and the
     * parsed pieces (not the client's URL) are what the engine client later uses.
     */
    private Sel parseTarget(String raw) throws MiniHttpServer.HttpError {
        final MiniHttpServer.HttpError denied = new MiniHttpServer.HttpError(403, "Target not allowed");
        if (raw.length() > 8192) throw denied;
        URI u;
        try {
            u = new URI(raw);
        } catch (URISyntaxException e) {
            throw denied;
        }
        String scheme = u.getScheme();
        if (!"http".equals(scheme) && !"https".equals(scheme)) throw denied;
        if (u.getRawUserInfo() != null || u.getRawFragment() != null) throw denied;
        if (!cfg.engineHost.equals(u.getHost()) || (u.getPort() != cfg.contractEnginePort 
                && u.getPort() != cfg.enginePort)) throw denied;
        String rp = u.getRawPath();
        if (rp == null || !rp.startsWith("/stream/")) throw denied;
        String seg = rp.substring(8);
        if (seg.isEmpty() || seg.indexOf('/') >= 0) throw denied;
        MagnetUtil.Parsed pm;
        try {
            pm = MagnetUtil.parse(pctDecode(seg));
        } catch (IllegalArgumentException e) {
            throw new MiniHttpServer.HttpError(400, "invalid magnet: " + e.getMessage());
        }
        String file = null;
        int idx = 0;
        String q = u.getRawQuery();
        if (q != null) {
            for (String kv : q.split("&")) {
                int eq = kv.indexOf('=');
                if (eq <= 0) continue;
                String k = kv.substring(0, eq);
                String v;
                try {
                    v = URLDecoder.decode(kv.substring(eq + 1), "UTF-8");
                } catch (Exception e) {
                    continue;
                }
                if (k.equals("file") && file == null && v.length() <= 512) file = v;
                else if (k.equals("fileIndex") && idx == 0 && v.length() <= 6) {
                    try {
                        idx = Math.max(0, Integer.parseInt(v.trim()));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        return new Sel(pm, file, idx);
    }

    /** Percent-decoding for a path segment ('+' stays a plus). */
    private static String pctDecode(String s) {
        try {
            return URLDecoder.decode(s.replace("+", "%2B"), "UTF-8");
        } catch (Exception e) {
            throw new IllegalArgumentException("bad percent-encoding");
        }
    }

    // ------------------------------------------------------------------ /probe and /warm

    private void jsonError(MiniHttpServer.Response res, int code, String msg) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", false);
        m.put("error", msg);
        res.text(code, "application/json", Json.write(m));
    }

    private Ref refOrFail(MiniHttpServer.Request req, MiniHttpServer.Response res, Sel[] out) throws IOException {
        try {
            out[0] = selection(req);
        } catch (MiniHttpServer.HttpError e) {
            jsonError(res, e.status, e.getMessage());
            return null;
        }
        try {
            return resolve(out[0]);
        } catch (IOException e) {
            jsonError(res, e instanceof TimeoutLike ? 504 : 502, String.valueOf(e.getMessage()));
            return null;
        }
    }

    private static String param(MiniHttpServer.Request req, String k) {
        String v = req.query.get(k);
        return v == null ? "" : (v.length() > 40 ? v.substring(0, 40) : v);
    }

    /** Measures real startup throughput by downloading the first 3 blocks (12 MiB at defaults). */
    private void probe(MiniHttpServer.Request req, MiniHttpServer.Response res) throws Exception {
        Sel[] sel = new Sel[1];
        Ref ref = refOrFail(req, res, sel);
        if (ref == null) return;
        final String quality = param(req, "quality");
        long t0 = System.nanoTime();
        long bytes = 0;
        int blocks = 0;
        Flow fl = flows.computeIfAbsent(ref.key(), k -> new Flow());
        try {
            for (int i = 0; i < 3 && i < ref.blocks(cfg.blockSize); i++) {
                try (BlockCache.Lease l = getBlock(ref, i)) {
                    bytes += l.file.length();
                    blocks++;
                }
            }
        } catch (IOException e) {
            jsonError(res, e instanceof TimeoutLike ? 504 : 502, String.valueOf(e.getMessage()));
            return;
        }
        fl.lastBlock = -1; // the real playback will start at block 0: that is not a seek
        long ms = Math.max(1, (System.nanoTime() - t0) / 1_000_000L);
        long rate = Math.round(bytes / (ms / 1000.0));
        double required = 1.5 * 1024 * 1024;
        if (quality.equals("2160p")) required = 4.0 * 1024 * 1024;
        else if (quality.equals("720p")) required = 0.9 * 1024 * 1024;
        else if (quality.equals("480p")) required = 0.5 * 1024 * 1024;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", true);
        m.put("stable", blocks >= 2 && rate >= required);
        m.put("stabilityScore", Math.max(0, Math.min(100, Math.round(rate / required * 100))));
        m.put("throughputBytesPerSec", rate);
        m.put("requiredBytesPerSec", Math.round(required));
        m.put("bytes", bytes);
        m.put("blocks", blocks);
        m.put("warmedMiB", Math.round(bytes / 1024.0 / 1024.0));
        m.put("ms", ms);
        m.put("language", param(req, "lang"));
        m.put("quality", quality);
        m.put("codec", param(req, "codec"));
        m.put("cache", "ready");
        res.text(200, "application/json", Json.write(m));
    }

    /** Pre-loads the first two blocks and keeps prefetching in the background. */
    private void warm(MiniHttpServer.Request req, MiniHttpServer.Response res) throws Exception {
        Sel[] sel = new Sel[1];
        Ref ref = refOrFail(req, res, sel);
        if (ref == null) return;
        long t0 = System.nanoTime();
        int total = ref.blocks(cfg.blockSize);
        long bytes = 0;
        try {
            if (total > 1) task(ref, 1, true); // start block 1 in parallel with block 0
            try (BlockCache.Lease l = getBlock(ref, 0)) {
                bytes += l.file.length();
            }
            if (total > 1) {
                try (BlockCache.Lease l = getBlock(ref, 1)) {
                    bytes += l.file.length();
                } catch (IOException ignored) {
                    // block 0 is what makes the start possible; block 1 is a bonus
                }
            }
        } catch (IOException e) {
            jsonError(res, e instanceof TimeoutLike ? 504 : 502, String.valueOf(e.getMessage()));
            return;
        }
        Flow fl = flows.computeIfAbsent(ref.key(), k -> new Flow());
        fl.lastBlock = -1;
        prefetch(ref, fl, 2, cfg.prefetchMinBlocks, total - 1);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", true);
        m.put("warmedMiB", Math.round(bytes / 1024.0 / 1024.0));
        m.put("sizeBytes", ref.size);
        m.put("ms", Math.max(1, (System.nanoTime() - t0) / 1_000_000L));
        m.put("language", param(req, "lang"));
        m.put("quality", param(req, "quality"));
        m.put("codec", param(req, "codec"));
        m.put("cache", "ready");
        res.text(200, "application/json", Json.write(m));
    }

    private void streamBlocks(Ref ref, long start, long end, OutputStream out, BlockCache.Lease firstLease) throws IOException {
        final int bs = cfg.blockSize;
        final int lastFileBlock = ref.blocks(bs) - 1;
        int firstBlock = (int) (start / bs);
        int lastBlock = (int) (end / bs);
        Flow fl = flows.computeIfAbsent(ref.key(), k -> new Flow());

        boolean sequential = firstBlock >= fl.lastBlock - 1 && firstBlock <= fl.lastBlock + 2;
        int boost = 0;
        if (!sequential) { // seek: drop stale read-ahead and push the new position first
            for (BlockTask t : fl.pending) t.cancelled = true;
            fl.pending.clear();
            fl.run = 0;
            boost = cfg.seekBoostBlocks;
            Logx.i(TAG, "seek -> block " + firstBlock + " of " + ref.name);
        }
        BlockCache.Lease lease = firstLease;
        try {
            for (int b = firstBlock; b <= lastBlock; b++) {
                int window = boost > 0 ? boost : Math.min(cfg.prefetchMaxBlocks, cfg.prefetchMinBlocks + fl.run / 2);
                boost = 0;
                prefetch(ref, fl, b + 1, window, lastFileBlock);
                if (lease == null) lease = getBlock(ref, b);
                long blockStart = (long) b * bs;
                long from = Math.max(start, blockStart) - blockStart;
                long to = Math.min(end, blockStart + bs - 1) - blockStart;
                copy(lease.file, from, to - from + 1, out);
                lease.close();
                lease = null;
                fl.lastBlock = b;
                fl.run++;
            }
        } finally {
            if (lease != null) lease.close();
        }
    }

    private static void copy(File f, long from, long len, OutputStream out) throws IOException {
        try (FileInputStream in = new FileInputStream(f)) {
            in.getChannel().position(from);
            byte[] buf = new byte[64 * 1024];
            long left = len;
            while (left > 0) {
                int n = in.read(buf, 0, (int) Math.min(buf.length, left));
                if (n < 0) throw new IOException("cached block shorter than expected");
                out.write(buf, 0, n);
                left -= n;
            }
        }
    }

    // ------------------------------------------------------------------ blocks

    private final class BlockTask implements Runnable {
        final String key;
        final Ref ref;
        final int block;
        final CompletableFuture<Void> done = new CompletableFuture<>();
        final AtomicBoolean claimed = new AtomicBoolean();
        volatile boolean cancelled;

        BlockTask(String key, Ref ref, int block) {
            this.key = key;
            this.ref = ref;
            this.block = block;
        }

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) return;
            try {
                if (cancelled) {
                    done.completeExceptionally(new CancellationException());
                    return;
                }
                if (!cache.has(key)) download();
                done.complete(null);
            } catch (Throwable t) {
                done.completeExceptionally(t);
            } finally {
                inflight.remove(key, this);
            }
        }

        private void download() throws IOException {
            long start = (long) block * cfg.blockSize;
            long len = Math.min(cfg.blockSize, ref.size - start);
            File tmp = cache.newTmp();
            try {
                engine.fetchRange(ref.hash, ref.fileIndex, start, len, tmp);
                cache.commit(key, tmp, len);
            } catch (IOException | RuntimeException e) {
                tmp.delete();
                throw e;
            }
        }
    }

    private BlockTask task(Ref ref, int block, boolean demand) throws IOException {
        String key = BlockCache.key(ref.hash, ref.fileIndex, block);
        boolean[] created = {false};
        BlockTask t = inflight.compute(key, (k, old) -> {
            if (old != null && !old.cancelled) return old;
            created[0] = true;
            return new BlockTask(k, ref, block);
        });
        try {
            if (demand) {
                if (created[0] || !t.claimed.get()) demandPool.execute(t);
            } else if (created[0]) {
                prefetchPool.execute(t);
            }
        } catch (RejectedExecutionException e) {
            inflight.remove(key, t);
            throw new IOException("gateway is shutting down");
        }
        return t;
    }

    /** Returns a pinned cache lease for the block, downloading it first if needed. */
    private BlockCache.Lease getBlock(Ref ref, int block) throws IOException {
        String key = BlockCache.key(ref.hash, ref.fileIndex, block);
        BlockCache.Lease l = cache.acquire(key);
        if (l != null) return l;
        cache.reserve(key); // keeps the block pinned from the moment it lands until we take it
        boolean ok = false;
        try {
            for (int i = 0; i < 4; i++) {
                l = cache.claim(key);
                if (l != null) {
                    ok = true;
                    return l;
                }
                if (closed) throw new IOException("gateway closed");
                BlockTask t = task(ref, block, true);
                try {
                    t.done.get(cfg.pieceWaitTimeoutMs + 30_000, TimeUnit.MILLISECONDS);
                } catch (CancellationException ce) {
                    continue; // a prefetch was cancelled by a seek while we waited: ask again as demand
                } catch (ExecutionException e) {
                    Throwable c = e.getCause();
                    if (c instanceof CancellationException) continue;
                    if (c instanceof IOException) throw (IOException) c;
                    throw new IOException(String.valueOf(c), c);
                } catch (TimeoutException e) {
                    throw new TimeoutLike("timed out fetching block " + block);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted");
                }
            }
            l = cache.claim(key);
            if (l == null) throw new IOException("block " + block + " unavailable");
            ok = true;
            return l;
        } finally {
            if (!ok) cache.unreserve(key);
        }
    }

    private void prefetch(Ref ref, Flow fl, int from, int count, int lastFileBlock) {
        for (int b = from; b < from + count && b <= lastFileBlock; b++) {
            String key = BlockCache.key(ref.hash, ref.fileIndex, b);
            if (cache.has(key) || inflight.containsKey(key)) continue;
            try {
                BlockTask t = task(ref, b, false);
                fl.pending.add(t);
                t.done.whenComplete((v, ex) -> fl.pending.remove(t));
            } catch (IOException e) {
                return;
            }
        }
    }

    private static String contentType(String name) {
        String n = name.toLowerCase(Locale.ROOT);
        if (n.endsWith(".mp4") || n.endsWith(".m4v")) return "video/mp4";
        if (n.endsWith(".mkv")) return "video/x-matroska";
        if (n.endsWith(".webm")) return "video/webm";
        if (n.endsWith(".avi")) return "video/x-msvideo";
        if (n.endsWith(".mov")) return "video/quicktime";
        if (n.endsWith(".ts")) return "video/mp2t";
        return "application/octet-stream";
    }

    @Override
    public void close() {
        closed = true;
        if (server != null) server.close();
        for (BlockTask t : inflight.values()) t.cancelled = true;
        if (demandPool != null) demandPool.shutdownNow();
        if (prefetchPool != null) prefetchPool.shutdownNow();
        inflight.clear();
        refs.clear();
        flows.clear();
    }
}
