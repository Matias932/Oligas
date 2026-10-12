import com.stroku.p2p.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Pure-JVM self-test of the HTTP stack (MiniHttpServer, EngineServer, EngineClient, Gateway,
 * BlockCache, MagnetUtil, ByteRange) against an in-memory fake torrent backend. It does NOT test
 * libtorrent4j or Android: those can only be exercised on a device.
 */
public class GatewaySelfTest {
    static final String HASH1 = "0123456789abcdef0123456789abcdef01234567";
    static final String HASH2 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    static final long SIZE1 = 5L * 1024 * 1024 + 123;
    static final long SIZE2 = 1024 * 1024;
    static final int PIECE = 64 * 1024;

    static int pass = 0, fail = 0;

    static void check(boolean ok, String what) {
        if (ok) {
            pass++;
            System.out.println("  PASS  " + what);
        } else {
            fail++;
            System.out.println("  FAIL  " + what);
        }
    }

    static byte expected(long pos) {
        return (byte) ((pos * 31 + 7) & 0xff);
    }

    // ---------------------------------------------------------------- fake backend
    static final class FakeBackend implements TorrentBackend {
        final Map<String, Meta> metas = new ConcurrentHashMap<>();
        final Map<String, Long> readyAt = new ConcurrentHashMap<>();
        final List<String> magnets = Collections.synchronizedList(new ArrayList<>());
        final List<long[]> prios = Collections.synchronizedList(new ArrayList<>());

        FakeBackend() {
            metas.put(HASH1, new Meta(HASH1, "Movie", PIECE, Arrays.asList(
                    new FileMeta(0, "Movie/movie.mkv", SIZE1), new FileMeta(1, "Movie/readme.nfo", 100)), 0));
            metas.put(HASH2, new Meta(HASH2, "Broken", PIECE, Arrays.asList(
                    new FileMeta(0, "Broken/broken.mp4", SIZE2)), 0));
        }

        public Meta addMagnet(String m, long t) throws IOException {
            magnets.add(m);
            MagnetUtil.Parsed p = MagnetUtil.parse(m);
            Meta meta = metas.get(p.infoHash);
            if (meta == null) throw new IOException("no metadata (fake)");
            return meta;
        }

        public Meta meta(String h) { return metas.get(h); }

        public void prioritize(String h, int f, long off, long len) {
            prios.add(new long[]{off, len});
            long first = off / PIECE, last = (off + len - 1) / PIECE;
            long now = System.nanoTime();
            for (long p = first; p <= last; p++) {
                readyAt.putIfAbsent(h + "/" + p, now + 5_000_000L); // piece "arrives" 5 ms after being requested
            }
        }

        public long availableFrom(String h, int f, long off, long maxLen) {
            long size = metas.get(h).files.get(f).size;
            long pos = off, n = 0, now = System.nanoTime();
            while (n < maxLen && pos < size) {
                Long t = readyAt.get(h + "/" + (pos / PIECE));
                if (t == null || now < t) break;
                long chunk = Math.min(PIECE - (pos % PIECE), Math.min(maxLen - n, size - pos));
                n += chunk;
                pos += chunk;
            }
            return n;
        }

        public int read(String h, int f, long off, byte[] buf, int o, int len) throws IOException {
            if (h.equals(HASH2) && off >= 100_000) throw new IOException("injected disk failure");
            for (int i = 0; i < len; i++) buf[o + i] = expected(off + i);
            return len;
        }

        public Map<String, Object> status() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("engine", "fake");
            m.put("running", true);
            m.put("torrents", metas.size());
            m.put("peers", 7);
            m.put("downloadRate", 123456L);
            return m;
        }

        public void close() {}
    }

    // ---------------------------------------------------------------- http helpers
    static final class Resp {
        int code;
        Map<String, List<String>> headers;
        byte[] body = new byte[0];

        String h(String n) {
            for (Map.Entry<String, List<String>> e : headers.entrySet()) {
                if (e.getKey() != null && e.getKey().equalsIgnoreCase(n)) return e.getValue().get(0);
            }
            return null;
        }
    }

    static Resp http(String method, String url, String range) throws IOException {
        HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        c.setInstanceFollowRedirects(false);
        c.setConnectTimeout(3000);
        c.setReadTimeout(60000);
        c.setRequestMethod(method);
        if (range != null) c.setRequestProperty("Range", range);
        Resp r = new Resp();
        r.code = c.getResponseCode();
        r.headers = c.getHeaderFields();
        InputStream in = r.code >= 400 ? c.getErrorStream() : c.getInputStream();
        if (in != null && !method.equals("HEAD")) {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            byte[] b = new byte[16384];
            int n;
            try {
                while ((n = in.read(b)) >= 0) bo.write(b, 0, n);
            } finally {
                in.close();
            }
            r.body = bo.toByteArray();
        }
        c.disconnect();
        return r;
    }

    static boolean bodyMatches(byte[] body, long startPos) {
        for (int i = 0; i < body.length; i++) if (body[i] != expected(startPos + i)) return false;
        return true;
    }

    static String rawRequest(int port, String request) throws IOException {
        try (Socket s = new Socket("127.0.0.1", port)) {
            s.setSoTimeout(5000);
            s.getOutputStream().write(request.getBytes("ISO-8859-1"));
            s.getOutputStream().flush();
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream(), "ISO-8859-1"));
            String line = br.readLine();
            return line == null ? "" : line;
        }
    }

    static String magnet(String hash) {
        return "magnet:?xt=urn:btih:" + hash + "&dn=Test+Movie&tr=" + URLEncoder.encode("udp://tracker.example.org:6969/announce");
    }

    static String play(int port, String m) throws IOException {
        return "http://127.0.0.1:" + port + "/play?magnet=" + URLEncoder.encode(m, "UTF-8");
    }

    /** Prefetches that are still downloading legitimately own a .tmp file; wait for them to finish. */
    static int tmpFilesAfterSettling(File dir) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 8000;
        int n;
        while ((n = files(dir, ".tmp").length) > 0 && System.currentTimeMillis() < deadline) Thread.sleep(100);
        return n;
    }

    static File[] files(File dir, String suffix) {
        File[] fs = dir.listFiles((d, n) -> n.endsWith(suffix));
        return fs == null ? new File[0] : fs;
    }

    // ---------------------------------------------------------------- main
    public static void main(String[] args) throws Exception {
        Logx.setSink((l, tag, m, t) -> { /* keep output readable */ });
        Path tmp = Files.createTempDirectory("stroku-selftest");
        Config cfg = new Config();
        cfg.gatewayBind = "127.0.0.1";
        cfg.gatewayPort = 0;
        cfg.enginePort = 0;
        cfg.blockSize = 256 * 1024;
        cfg.maxCache = 2L * 1024 * 1024;
        cfg.prefetchMinBlocks = 2;
        cfg.prefetchMaxBlocks = 4;
        cfg.seekBoostBlocks = 4;
        cfg.parallelFetches = 3;
        cfg.pieceWaitTimeoutMs = 5000;
        cfg.metadataTimeoutMs = 5000;
        cfg.cacheDir = tmp.resolve("cache").toFile();

        FakeBackend backend = new FakeBackend();
        EngineServer engine = new EngineServer(cfg, backend);
        engine.start();
        BlockCache cache = new BlockCache(cfg.cacheDir, cfg.maxCache);
        Gateway gw = new Gateway(cfg, cache);
        gw.start();
        int gp = gw.port();
        String base = "http://127.0.0.1:" + gp;
        System.out.println("engine on 127.0.0.1:" + cfg.enginePort + ", gateway on 127.0.0.1:" + gp);

        System.out.println("\n[1] listening + health");
        check(NetUtil.tcpOpen("127.0.0.1", gp, 1000), "gateway port accepts TCP");
        check(NetUtil.tcpOpen("127.0.0.1", cfg.enginePort, 1000), "engine port accepts TCP");
        Resp h = http("GET", base + "/health", null);
        String hb = new String(h.body, "UTF-8");
        check(h.code == 200 && hb.contains("\"ok\":true") && hb.contains("\"engineOk\":true"), "gateway /health reports engine ok: " + hb);
        StatusProbe.Report rep = StatusProbe.run(cfg);
        check(rep.engineHealthOk && rep.gatewayHealthOk && rep.engineListening && rep.gatewayListening, "StatusProbe sees both services");

        System.out.println("\n[2] HTTP Range / 206");
        String url = play(gp, magnet(HASH1));
        Resp r = http("GET", url, "bytes=0-99");
        check(r.code == 206 && r.h("Content-Range").equals("bytes 0-99/" + SIZE1) && r.body.length == 100 && bodyMatches(r.body, 0),
                "bytes=0-99 -> 206 with right Content-Range and bytes");
        r = http("GET", url, "bytes=262140-262150");
        check(r.code == 206 && r.body.length == 11 && bodyMatches(r.body, 262140), "range across a block boundary");
        r = http("GET", url, "bytes=-100");
        check(r.code == 206 && r.body.length == 100 && bodyMatches(r.body, SIZE1 - 100), "suffix range bytes=-100");
        r = http("GET", url, "bytes=4000000-");
        check(r.code == 206 && r.body.length == SIZE1 - 4000000 && bodyMatches(r.body, 4000000), "open-ended range to EOF (seek + multi block)");
        r = http("GET", url, "bytes=99999999999-");
        check(r.code == 416 && r.h("Content-Range").equals("bytes */" + SIZE1), "unsatisfiable range -> 416");
        r = http("GET", url, null);
        check(r.code == 200 && r.body.length == SIZE1 && bodyMatches(r.body, 0) && "bytes".equals(r.h("Accept-Ranges")), "no Range -> 200 full file, bytes identical");
        r = http("HEAD", url, null);
        check(r.code == 200 && String.valueOf(SIZE1).equals(r.h("Content-Length")) && r.body.length == 0, "HEAD -> headers only");

        System.out.println("\n[3] seek behaviour");
        backend.prios.clear();
        r = http("GET", url, "bytes=1000000-1000099");
        long nearSeek = 0;
        for (long[] p : backend.prios) if (Math.abs(p[0] - 1000000) < 300000) nearSeek++;
        check(r.code == 206 && bodyMatches(r.body, 1000000) && nearSeek > 0, "seek prioritises the engine near the new offset");

        System.out.println("\n[4] cache");
        long cached = cache.totalBytes();
        check(cached <= cfg.maxCache, "cache stays within limit: " + cached + " <= " + cfg.maxCache);
        check(tmpFilesAfterSettling(cfg.cacheDir) == 0, "no leftover .tmp files once in-flight work settles");
        boolean sizesOk = true;
        for (File f : files(cfg.cacheDir, ".blk")) {
            long len = f.length();
            if (!f.exists()) continue; // evicted (deleted) while we were looking: not a defect
            String n = f.getName();
            int blk = Integer.parseInt(n.substring(n.lastIndexOf('-') + 1, n.length() - 4));
            long exp = Math.min(cfg.blockSize, SIZE1 - (long) blk * cfg.blockSize);
            if (len != exp) {
                sizesOk = false;
                System.out.println("      BAD BLOCK " + n + " len=" + len + " expected=" + exp + " exists=" + f.exists());
            }
        }
        check(sizesOk && files(cfg.cacheDir, ".blk").length > 0, "every cached block has exactly the expected size");

        System.out.println("\n[5] errors: incomplete data is never cached");
        int blocksBefore = files(cfg.cacheDir, ".blk").length;
        r = http("GET", play(gp, magnet(HASH2)), "bytes=0-99");
        check(r.code == 502 || r.code == 504, "engine failing mid-block -> HTTP " + r.code + " (not a bogus 206)");
        check(tmpFilesAfterSettling(cfg.cacheDir) == 0, "failed block left no .tmp file");
        boolean h2cached = false;
        for (File f : files(cfg.cacheDir, ".blk")) if (f.getName().startsWith(HASH2)) h2cached = true;
        check(!h2cached, "incomplete block was NOT committed to the cache");
        r = http("GET", play(gp, magnet("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")), "bytes=0-9");
        check(r.code == 504 || r.code == 502, "torrent without metadata -> HTTP " + r.code);
        // BlockCache itself must refuse a wrong-sized file
        File t = cache.newTmp();
        Files.write(t.toPath(), new byte[10]);
        boolean refused = false;
        try {
            cache.commit("x-0-0", t, 11);
        } catch (IOException e) {
            refused = true;
        }
        check(refused && !cache.has("x-0-0"), "BlockCache.commit rejects a short block");

        System.out.println("\n[6] security");
        check(http("GET", "http://127.0.0.1:" + gp + "/play?magnet=" + URLEncoder.encode("http://evil.example/x", "UTF-8"), null).code == 400, "non-magnet URL rejected (400)");
        check(http("GET", "http://127.0.0.1:" + gp + "/play?magnet=" + URLEncoder.encode("magnet:?xt=urn:btih:zz", "UTF-8"), null).code == 400, "magnet with bad hash rejected (400)");
        check(http("GET", "http://127.0.0.1:" + gp + "/play", null).code == 400, "missing magnet rejected (400)");
        check(rawRequest(gp, "GET http://127.0.0.1:9/ HTTP/1.1\r\nHost: x\r\n\r\n").contains(" 400 "), "absolute-form (proxy-style) request rejected");
        check(rawRequest(gp, "CONNECT example.com:443 HTTP/1.1\r\nHost: x\r\n\r\n").contains(" 405 "), "CONNECT rejected (405)");
        check(http("GET", base + "/stream/" + HASH1 + "/0", null).code == 404, "engine-style path is not exposed on the gateway");
        check(http("GET", base + "/../../etc/passwd", null).code == 404, "path traversal -> 404");
        String evil = "magnet:?xt=urn:btih:cccccccccccccccccccccccccccccccccccccccc" + "&ws=" + URLEncoder.encode("http://127.0.0.1:9/seed", "UTF-8")
                + "&xs=" + URLEncoder.encode("http://127.0.0.1:9/t.torrent", "UTF-8")
                + "&tr=" + URLEncoder.encode("udp://127.0.0.1:9/announce", "UTF-8")
                + "&tr=" + URLEncoder.encode("http://192.168.1.5/announce", "UTF-8")
                + "&tr=" + URLEncoder.encode("udp://good.example.org:80/announce", "UTF-8");
        backend.magnets.clear();
        http("GET", play(gp, evil) + "&x=1", "bytes=0-0");
        String fwd = backend.magnets.isEmpty() ? "" : backend.magnets.get(backend.magnets.size() - 1);
        check(!backend.magnets.isEmpty() && !fwd.contains("ws=") && !fwd.contains("xs=") && !fwd.contains("127.0.0.1")
                && !fwd.contains("192.168") && fwd.contains("good.example.org"), "ws/xs and private trackers are stripped before reaching the engine: " + fwd);
        // engine requires its token
        check(http("GET", "http://127.0.0.1:" + cfg.enginePort + "/resolve?magnet=" + URLEncoder.encode(magnet(HASH1), "UTF-8"), null).code == 403, "engine rejects requests without token (403)");
        // redirects from a hostile 'engine' are refused and never followed
        AtomicBoolean trapHit = new AtomicBoolean();
        ServerSocket trap = new ServerSocket(0, 5, InetAddress.getByName("127.0.0.1"));
        Thread tt = new Thread(() -> {
            try {
                trap.accept().close();
                trapHit.set(true);
            } catch (IOException ignored) {
            }
        });
        tt.setDaemon(true);
        tt.start();
        MiniHttpServer fake = new MiniHttpServer("fake", 4, 10000, a -> true, (q, s) -> {
            s.status(302).header("Location", "http://127.0.0.1:" + trap.getLocalPort() + "/x");
            s.text(302, "text/plain", "moved");
        });
        fake.start("127.0.0.1", 0);
        Config bad = new Config();
        bad.enginePort = fake.getPort();
        boolean refusedRedirect = false;
        try {
            new EngineClient(bad).fetchRange(HASH1, 0, 0, 10, new File(tmp.toFile(), "x.tmp"));
        } catch (IOException e) {
            refusedRedirect = e.getMessage().contains("redirect");
        }
        Thread.sleep(300);
        check(refusedRedirect && !trapHit.get(), "redirect from upstream refused and not followed");
        fake.close();
        trap.close();
        check(ByteRange.parse("bytes=0-1,5-9", 100) == null, "multi-range ignored (served as full body)");
        check(MagnetUtil.parse("magnet:?xt=urn:btih:VGZXYNUWWFVEMWQTHV3KFA2TNAYIEB5P").infoHash.length() == 40, "base32 info-hash converted to hex");

        System.out.println("\n[7] concurrency");
        ExecutorService ex = Executors.newFixedThreadPool(8);
        List<Future<String>> fs = new ArrayList<>();
        Random rnd = new Random(42);
        for (int i = 0; i < 16; i++) {
            final long s = (long) (rnd.nextDouble() * (SIZE1 - 600_000));
            fs.add(ex.submit(() -> {
                try {
                    Resp rr = http("GET", url, "bytes=" + s + "-" + (s + 499_999));
                    if (rr.code == 206 && rr.body.length == 500_000 && bodyMatches(rr.body, s)) return null;
                    return "start=" + s + " code=" + rr.code + " len=" + rr.body.length + " match=" + bodyMatches(rr.body, s);
                } catch (Exception e) {
                    return "start=" + s + " exception " + e;
                }
            }));
        }
        boolean allOk = true;
        for (Future<String> f : fs) {
            String err = f.get(120, TimeUnit.SECONDS);
            if (err != null) {
                allOk = false;
                System.out.println("      concurrent failure: " + err);
            }
        }
        ex.shutdown();
        check(allOk, "16 concurrent random ranges all correct");
        check(cache.totalBytes() <= cfg.maxCache, "cache limit still respected after concurrency: " + cache.totalBytes());
        check(tmpFilesAfterSettling(cfg.cacheDir) == 0, "no leftover .tmp files after the concurrency burst");

        System.out.println("\n[8] shutdown");
        gw.close();
        engine.close();
        Thread.sleep(300);
        check(!NetUtil.tcpOpen("127.0.0.1", gp, 500), "gateway port closed after stop");
        check(!NetUtil.tcpOpen("127.0.0.1", cfg.enginePort, 500), "engine port closed after stop");
        cache.clearAll();
        check(files(cfg.cacheDir, ".blk").length == 0, "cache cleared on stop");

        System.out.println("\nRESULT: " + pass + " passed, " + fail + " failed");
        System.exit(fail == 0 ? 0 : 1);
    }
}
