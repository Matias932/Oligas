package com.stroku.p2p;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * On-disk LRU block cache with a hard byte limit. A block only becomes visible after it was written
 * to a temp file, verified to have exactly the expected length, and atomically renamed, so partial
 * data is never served. Blocks in use by a response are pinned and cannot be evicted.
 */
public final class BlockCache {
    private static final class Entry {
        final File file;
        final long size;
        int pins;

        Entry(File f, long s) {
            file = f;
            size = s;
        }
    }

    /** Pinned handle on a cached block; close() releases the pin. */
    public final class Lease implements AutoCloseable {
        public final File file;
        private final String key;
        private boolean closed;

        Lease(String key, File file) {
            this.key = key;
            this.file = file;
        }

        @Override
        public void close() {
            synchronized (BlockCache.this) {
                if (closed) return;
                closed = true;
                Entry e = index.get(key);
                if (e != null && e.pins > 0) e.pins--;
                evictLocked();
            }
        }
    }

    private final File dir;
    private final long maxBytes;
    private final LinkedHashMap<String, Entry> index = new LinkedHashMap<>(64, 0.75f, true);
    private final AtomicLong tmpSeq = new AtomicLong();
    /** Pins promised to waiters of blocks that are still being downloaded. */
    private final Map<String, Integer> reserved = new HashMap<>();
    private long total;

    public BlockCache(File dir, long maxBytes) throws IOException {
        this.dir = dir;
        this.maxBytes = maxBytes;
        if (!dir.isDirectory() && !dir.mkdirs()) throw new IOException("cannot create cache dir " + dir);
        clearAll(); // cache is per server run: unknown leftovers could be partial blocks
    }

    public static String key(String hash, int fileIdx, int block) {
        return hash + "-" + fileIdx + "-" + block;
    }

    public File newTmp() {
        return new File(dir, "t" + System.nanoTime() + "-" + tmpSeq.incrementAndGet() + ".tmp");
    }

    public synchronized boolean has(String key) {
        return index.containsKey(key);
    }

    public synchronized Lease acquire(String key) {
        Entry e = index.get(key); // access order: refreshes LRU position
        if (e == null) return null;
        e.pins++;
        return new Lease(key, e.file);
    }

    /**
     * Promises a pin for a block that a request is about to wait for, so the block cannot be
     * evicted between "download finished" and "request got hold of it". Must be paired with
     * claim() (success) or unreserve() (failure).
     */
    public synchronized void reserve(String key) {
        Entry e = index.get(key);
        if (e != null) e.pins++;
        else reserved.merge(key, 1, Integer::sum);
    }

    /** Turns a reservation into a lease (no extra pin) once the block exists; null if not yet there. */
    public synchronized Lease claim(String key) {
        Entry e = index.get(key);
        return e == null ? null : new Lease(key, e.file);
    }

    public synchronized void unreserve(String key) {
        Entry e = index.get(key);
        if (e != null) {
            if (e.pins > 0) e.pins--;
            evictLocked();
            return;
        }
        Integer n = reserved.get(key);
        if (n != null) {
            if (n <= 1) reserved.remove(key);
            else reserved.put(key, n - 1);
        }
    }

    /** Publishes a finished temp file as block `key`; fails (and deletes it) if its length is wrong. */
    public synchronized void commit(String key, File tmp, long expectedLen) throws IOException {
        if (tmp.length() != expectedLen) {
            long got = tmp.length();
            tmp.delete();
            throw new IOException("refusing incomplete block " + key + ": " + got + "/" + expectedLen + " bytes");
        }
        if (index.containsKey(key)) {
            tmp.delete();
            return;
        }
        File dest = new File(dir, key + ".blk");
        if (!tmp.renameTo(dest)) {
            tmp.delete();
            throw new IOException("cannot publish block " + key);
        }
        Entry ne = new Entry(dest, expectedLen);
        Integer waiting = reserved.remove(key);
        if (waiting != null) ne.pins = waiting; // already pinned for the requests waiting on it
        index.put(key, ne);
        total += expectedLen;
        evictLocked();
    }

    private void evictLocked() {
        if (total <= maxBytes) return;
        Iterator<Map.Entry<String, Entry>> it = index.entrySet().iterator();
        while (total > maxBytes && it.hasNext()) {
            Map.Entry<String, Entry> me = it.next();
            Entry e = me.getValue();
            if (e.pins > 0) continue;
            it.remove();
            total -= e.size;
            e.file.delete();
        }
    }

    public synchronized long totalBytes() {
        return total;
    }

    public synchronized int blockCount() {
        return index.size();
    }

    public long maxBytes() {
        return maxBytes;
    }

    public synchronized void clearAll() {
        index.clear();
        reserved.clear();
        total = 0;
        File[] fs = dir.listFiles();
        if (fs != null) for (File f : fs) f.delete();
    }
}
