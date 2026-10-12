package com.stroku.p2p.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Caché en disco por bloques con límite de tamaño.
 *
 * Un bloque solo existe como ".blk" si se escribió completo: se descarga a ".part" y se renombra
 * (atómico) únicamente cuando su longitud coincide con la esperada. Al leer se vuelve a comprobar
 * la longitud, y los ".part" sobrantes de un arranque anterior se borran al iniciar.
 */
public final class BlockCache {
    private final File root;
    private final long maxBytes;
    private final long protectMs;
    private final AtomicLong total = new AtomicLong();

    public BlockCache(File root, long maxBytes, long protectMs) throws IOException {
        this.root = root;
        this.maxBytes = maxBytes;
        this.protectMs = protectMs;
        if (!root.isDirectory() && !root.mkdirs()) throw new IOException("no se pudo crear la caché: " + root);
        scan();
    }

    private void scan() {
        long sum = 0;
        File[] dirs = root.listFiles();
        if (dirs == null) return;
        for (File d : dirs) {
            File[] files = d.listFiles();
            if (!d.isDirectory() || files == null) continue;
            for (File f : files) {
                if (f.getName().endsWith(".part")) {
                    f.delete(); // descarga interrumpida: nunca se considera completa
                } else if (f.getName().endsWith(".blk")) {
                    sum += f.length();
                }
            }
        }
        total.set(sum);
    }

    public File dir(String hash, int file, long size) throws IOException {
        File d = new File(root, hash + "-" + file + "-" + size);
        if (!d.isDirectory() && !d.mkdirs() && !d.isDirectory()) throw new IOException("no se pudo crear " + d);
        return d;
    }

    public File blockFile(File dir, long block) {
        return new File(dir, block + ".blk");
    }

    public File partFile(File dir, long block) {
        return new File(dir, block + ".blk.part");
    }

    /** @return el bloque si existe completo; si existe con otro tamaño lo borra y devuelve null. */
    public File lookup(File dir, long block, int expected) {
        File f = blockFile(dir, block);
        if (!f.isFile()) return null;
        long len = f.length();
        if (len != expected) {
            if (f.delete()) total.addAndGet(-len);
            return null;
        }
        f.setLastModified(System.currentTimeMillis());
        return f;
    }

    /** Convierte un .part completo en bloque. */
    public synchronized void commit(File part, File dst, int expected) throws IOException {
        if (part.length() != expected) {
            part.delete();
            throw new IOException("bloque incompleto (" + part.length() + "/" + expected + ")");
        }
        long old = dst.isFile() ? dst.length() : 0;
        if (!part.renameTo(dst)) {
            part.delete();
            throw new IOException("no se pudo guardar el bloque");
        }
        total.addAndGet(expected - old);
        dst.setLastModified(System.currentTimeMillis());
        if (total.get() > maxBytes) evict();
    }

    private void evict() {
        final List<File> blocks = new ArrayList<>();
        File[] dirs = root.listFiles();
        if (dirs == null) return;
        for (File d : dirs) {
            File[] files = d.listFiles();
            if (!d.isDirectory() || files == null) continue;
            for (File f : files) if (f.getName().endsWith(".blk")) blocks.add(f);
        }
        // Recalcula el total real desde el disco: si algo borró bloques por fuera (p. ej. Android
        // purgando la caché) el contador no se queda desajustado.
        long actual = 0;
        for (File f : blocks) actual += f.length();
        total.set(actual);
        Collections.sort(blocks, new Comparator<File>() {
            @Override
            public int compare(File a, File b) {
                return Long.compare(a.lastModified(), b.lastModified());
            }
        });
        long target = (long) (maxBytes * 0.9);
        long now = System.currentTimeMillis();
        for (File f : blocks) {
            if (total.get() <= target) break;
            if (now - f.lastModified() < protectMs) continue; // en uso reciente
            long len = f.length();
            if (f.delete()) total.addAndGet(-len);
        }
    }

    public long totalBytes() {
        return total.get();
    }

    public long maxBytes() {
        return maxBytes;
    }
}
