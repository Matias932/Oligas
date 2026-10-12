package com.stroku.p2p.core;

/** Interpretación de la cabecera HTTP Range (un solo rango de bytes). */
public final class Ranges {
    private Ranges() {}

    /** Sin cabecera Range utilizable: se responde 200 con todo el contenido. */
    public static final long[] FULL = null;

    public static final class Unsatisfiable extends Exception {
        public Unsatisfiable(String m) {
            super(m);
        }
    }

    /**
     * @return {start, end} (ambos inclusivos) o null si la cabecera falta, no es de bytes o pide varios rangos.
     * @throws Unsatisfiable si el rango es válido pero queda fuera del archivo (416).
     */
    public static long[] parse(String header, long size) throws Unsatisfiable {
        if (header == null) return null;
        String h = header.trim();
        if (!h.regionMatches(true, 0, "bytes=", 0, 6)) return null;
        String spec = h.substring(6).trim();
        if (spec.isEmpty() || spec.indexOf(',') >= 0) return null;
        int dash = spec.indexOf('-');
        if (dash < 0) return null;
        String a = spec.substring(0, dash).trim();
        String b = spec.substring(dash + 1).trim();
        try {
            long start, end;
            if (a.isEmpty()) {
                if (b.isEmpty()) return null;
                long suffix = Long.parseLong(b);
                if (suffix <= 0) throw new Unsatisfiable("sufijo vacío");
                start = Math.max(0, size - suffix);
                end = size - 1;
            } else {
                start = Long.parseLong(a);
                end = b.isEmpty() ? size - 1 : Long.parseLong(b);
                if (start < 0) return null;
                if (end >= size) end = size - 1;
            }
            if (size == 0 || start >= size || end < start) throw new Unsatisfiable("fuera de rango");
            return new long[] {start, end};
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
