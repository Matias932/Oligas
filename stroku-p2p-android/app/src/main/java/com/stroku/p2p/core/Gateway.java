package com.stroku.p2p.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Gateway HTTP Range (puerto 4001, 0.0.0.0) para el Roku.
 *
 *   GET|HEAD /health
 *   GET|HEAD /stream?magnet=&lt;magnet urlencoded&gt;[&amp;file=N]     (o ?hash=&lt;40hex&gt; si ya se añadió)
 *
 * No es un proxy: solo existen esas dos rutas y el único destino de salida es el motor local
 * (UpstreamClient). Los datos se sirven desde una caché de bloques en disco con prefetch paralelo.
 */
public final class Gateway implements Closeable {

    private static final class FileState {
        final String hash;
        final int file;
        final long size;
        final String mime;
        final String magnet;
        final File dir;
        volatile long lastServed = Long.MIN_VALUE / 2;
        volatile int generation;
        volatile int window;

        FileState(String hash, int file, long size, String mime, String magnet, File dir, int window) {
            this.hash = hash;
            this.file = file;
            this.size = size;
            this.mime = mime;
            this.magnet = magnet;
            this.dir = dir;
            this.window = window;
        }

        int blockLen(long block, int blockSize) {
            long start = block * blockSize;
            return (int) Math.min(blockSize, size - start);
        }
    }

    private static final class Fetch {
        final String key;
        final CompletableFuture<File> future = new CompletableFuture<>();
        final AtomicBoolean claimed = new AtomicBoolean();

        Fetch(String key) {
            this.key = key;
        }
    }

    private final class Task implements Runnable, Comparable<Task> {
        final Fetch fetch;
        final FileState fs;
        final long block;
        final boolean demand;
        final int gen;
        final long prio;
        final long seq = seqGen.incrementAndGet();

        Task(Fetch fetch, FileState fs, long block, boolean demand, int gen, int distance) {
            this.fetch = fetch;
            this.fs = fs;
            this.block = block;
            this.demand = demand;
            this.gen = gen;
            this.prio = demand ? 0 : 10 + distance;
        }

        @Override
        public int compareTo(Task o) {
            int c = Long.compare(prio, o.prio);
            return c != 0 ? c : Long.compare(seq, o.seq);
        }

        @Override
        public void run() {
            if (fetch.future.isDone()) return;
            if (!fetch.claimed.compareAndSet(false, true)) return;
            if (!demand && gen != fs.generation) { // prefetch de una posición ya abandonada
                inflight.remove(fetch.key, fetch);
                fetch.future.cancel(false);
                return;
            }
            File part = null;
            try {
                int expected = fs.blockLen(block, cfg.blockSize);
                long start = block * (long) cfg.blockSize;
                byte[] buf = new byte[expected];
                upstream.fetchRange(fs.hash, fs.file, start, start + expected - 1, buf);
                part = cache.partFile(fs.dir, block);
                try (FileOutputStream fo = new FileOutputStream(part)) {
                    fo.write(buf, 0, expected);
                }
                File dst = cache.blockFile(fs.dir, block);
                cache.commit(part, dst, expected);
                fetch.future.complete(dst);
            } catch (Throwable t) {
                if (part != null) part.delete();
                fetch.future.completeExceptionally(t);
            } finally {
                inflight.remove(fetch.key, fetch);
            }
        }
    }

    private final Config cfg;
    private final UpstreamClient upstream;
    private final BlockCache cache;
    private final MiniHttp http;
    private final ThreadPoolExecutor fetchPool;
    private final AtomicLong seqGen = new AtomicLong();
    private final ConcurrentHashMap<String, Fetch> inflight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, FileState> files = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    public Gateway(Config cfg, BlockCache cache) {
        this.cfg = cfg;
        this.cache = cache;
        this.upstream = new UpstreamClient(Config.ENGINE_HOST, cfg.enginePort, cfg.resolveTimeoutMs, cfg.pieceTimeoutMs);
        final AtomicInteger n = new AtomicInteger();
        this.fetchPool = new ThreadPoolExecutor(cfg.parallelFetches, cfg.parallelFetches, 0L, TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "gateway-fetch-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        this.http = new MiniHttp("gateway", cfg.gatewayBind, cfg.gatewayPort, cfg.maxClientConnections,
                new MiniHttp.Handler() {
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

    @Override
    public void close() {
        http.close();
        fetchPool.shutdownNow();
        for (Fetch f : inflight.values()) f.future.cancel(true);
        inflight.clear();
    }

    // ---------------------------------------------------------------- rutas

    private void route(MiniHttp.Request req, MiniHttp.Response res) throws IOException {
        if (req.path.equals("/health")) {
            health(res);
        } else if (req.path.equals("/stream")) {
            stream(req, res);
        } else {
            res.error(404, "no encontrado");
        }
    }

    private void health(MiniHttp.Response res) throws IOException {
        Probe p = Probe.get("http://" + Config.ENGINE_HOST + ":" + cfg.enginePort + "/health", 2000);
        String up = p.ok() ? "{\"reachable\":true,\"ms\":" + p.millis + "}"
                : "{\"reachable\":false,\"error\":" + Json.str(p.error != null ? p.error : "HTTP " + p.code) + "}";
        res.json(200, "{\"ok\":true,\"gateway\":{\"port\":" + http.getPort()
                + ",\"blockSize\":" + cfg.blockSize
                + ",\"cacheBytes\":" + cache.totalBytes()
                + ",\"maxCacheBytes\":" + cache.maxBytes()
                + ",\"pendingFetches\":" + inflight.size() + "},\"upstream\":" + up + "}");
    }

    private FileState resolve(String hash, int file, String magnet) throws IOException {
        String key = hash + ":" + file;
        FileState fs = files.get(key);
        if (fs != null) return fs;
        Object lock = locks.computeIfAbsent(key, new java.util.function.Function<String, Object>() {
            @Override
            public Object apply(String k) {
                return new Object();
            }
        });
        synchronized (lock) {
            fs = files.get(key);
            if (fs != null) return fs;
            UpstreamClient.Info info = upstream.head(hash, file, magnet);
            File dir = cache.dir(hash, info.file, info.size);
            fs = new FileState(hash, info.file, info.size, info.mime, magnet, dir, cfg.prefetchMinBlocks);
            if (files.size() > 256) files.clear();
            files.put(key, fs);
            files.put(hash + ":" + info.file, fs);
            return fs;
        }
    }

    private void stream(MiniHttp.Request req, MiniHttp.Response res) throws IOException {
        String magnetParam = req.query.get("magnet");
        String hash;
        String magnet = null;
        if (magnetParam != null) {
            Magnets.Parsed p = Magnets.parse(magnetParam);
            if (p == null) {
                res.error(400, "magnet inválido");
                return;
            }
            hash = p.infoHash;
            magnet = p.magnet;
        } else {
            hash = Magnets.normalizeHash(req.query.get("hash"));
            if (hash == null) {
                res.error(400, "falta magnet o hash válido");
                return;
            }
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

        FileState fs;
        try {
            fs = resolve(hash, file, magnet);
        } catch (TorrentBackend.TimeoutIOException e) {
            res.error(504, e.getMessage());
            return;
        } catch (IOException e) {
            res.error(502, e.getMessage() == null ? "error del motor" : e.getMessage());
            return;
        }

        long[] range;
        try {
            range = Ranges.parse(req.headers.get("range"), fs.size);
        } catch (Ranges.Unsatisfiable e) {
            Map<String, String> h = new LinkedHashMap<>();
            h.put("Content-Range", "bytes */" + fs.size);
            res.start(416, 0, "text/plain", h);
            return;
        }
        boolean partial = range != null;
        long start = partial ? range[0] : 0;
        long end = partial ? range[1] : fs.size - 1;
        long length = fs.size == 0 ? 0 : end - start + 1;

        Map<String, String> h = new LinkedHashMap<>();
        if (partial) h.put("Content-Range", "bytes " + start + "-" + end + "/" + fs.size);
        // No se envían cabeceras hasta tener el primer bloque: si falla se puede responder 502 limpio.
        if (req.isHead() || length == 0) {
            res.start(partial ? 206 : 200, length, fs.mime, h);
            return;
        }
        serveBlocks(fs, start, end, partial, h, res);
    }

    // ------------------------------------------------------- bloques y prefetch

    private void serveBlocks(FileState fs, long start, long end, boolean partial, Map<String, String> headers,
                             MiniHttp.Response res) throws IOException {
        final long bs = cfg.blockSize;
        long first = start / bs;
        long last = end / bs;
        long lastBlockOfFile = (fs.size - 1) / bs;

        boolean sequential = first == fs.lastServed || first == fs.lastServed + 1;
        if (!sequential) {
            fs.generation++;           // los prefetch pendientes de la posición anterior se descartan
            fs.window = cfg.prefetchMinBlocks;
        }
        final int gen = fs.generation;
        boolean headersSent = false;

        for (long b = first; b <= last; b++) {
            int win = (b == first && !sequential) ? cfg.seekBoostBlocks : fs.window;
            Fetch demand = ensure(fs, b, true, gen, 0);
            for (int i = 1; i <= win; i++) {
                long p = b + i;
                if (p > lastBlockOfFile) break;
                ensure(fs, p, false, gen, i);
            }
            File blockFile;
            try {
                blockFile = await(fs, b, demand, gen);
            } catch (IOException e) {
                if (!headersSent) {
                    res.error(e instanceof TorrentBackend.TimeoutIOException ? 504 : 502,
                            e.getMessage() == null ? "error obteniendo datos" : e.getMessage());
                    return;
                }
                throw e; // a mitad de respuesta: se corta la conexión, nunca se rellena con datos falsos
            }
            if (!headersSent) {
                res.start(partial ? 206 : 200, end - start + 1, fs.mime, headers);
                headersSent = true;
            }
            long blockStart = b * bs;
            int blockLen = fs.blockLen(b, cfg.blockSize);
            long from = Math.max(start, blockStart) - blockStart;
            long to = Math.min(end, blockStart + blockLen - 1) - blockStart;
            copy(blockFile, from, to, res);
            fs.lastServed = b;
            if (b >= first && fs.window < cfg.prefetchMaxBlocks) fs.window++;
        }
    }

    private static void copy(File f, long from, long to, MiniHttp.Response res) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            raf.seek(from);
            byte[] buf = new byte[64 * 1024];
            long remaining = to - from + 1;
            while (remaining > 0) {
                int n = raf.read(buf, 0, (int) Math.min(buf.length, remaining));
                if (n < 0) throw new IOException("bloque en caché truncado");
                res.write(buf, 0, n);
                remaining -= n;
            }
        }
    }

    private Fetch ensure(FileState fs, long block, boolean demand, int gen, int distance) {
        int expected = fs.blockLen(block, cfg.blockSize);
        File cached = cache.lookup(fs.dir, block, expected);
        String key = fs.dir.getName() + "/" + block;
        if (cached != null) {
            Fetch done = new Fetch(key);
            done.future.complete(cached);
            return done;
        }
        Fetch f = inflight.get(key);
        boolean created = false;
        if (f == null) {
            Fetch nf = new Fetch(key);
            Fetch prev = inflight.putIfAbsent(key, nf);
            f = prev != null ? prev : nf;
            created = prev == null;
        }
        // Si ya existía y ahora se pide con urgencia, se encola otra tarea de máxima prioridad
        // sobre el mismo Fetch; el primero que la reclama hace el trabajo.
        if (created || demand) {
            try {
                fetchPool.execute(new Task(f, fs, block, demand, gen, distance));
            } catch (RuntimeException rejected) {
                inflight.remove(key, f);
                f.future.completeExceptionally(new IOException("gateway detenido"));
            }
        }
        return f;
    }

    private File await(FileState fs, long block, Fetch fetch, int gen) throws IOException {
        long waitMs = cfg.pieceTimeoutMs + 30_000;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                return fetch.future.get(waitMs, TimeUnit.MILLISECONDS);
            } catch (CancellationException ce) {
                fetch = ensure(fs, block, true, gen, 0); // un prefetch obsoleto lo canceló: se vuelve a pedir
            } catch (ExecutionException ee) {
                Throwable c = ee.getCause();
                if (c instanceof IOException) throw (IOException) c;
                throw new IOException(String.valueOf(c));
            } catch (TimeoutException te) {
                throw new TorrentBackend.TimeoutIOException("tiempo agotado esperando el bloque " + block);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrumpido");
            }
        }
        throw new IOException("no se pudo obtener el bloque " + block);
    }
}
