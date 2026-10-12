package com.stroku.p2p;

/** Single HTTP byte range (inclusive bounds). */
public final class ByteRange {
    /** Returned when the header is well formed but cannot be satisfied (HTTP 416). */
    public static final ByteRange INVALID = new ByteRange(-1, -1);

    public final long start;
    public final long end;

    private ByteRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public boolean isInvalid() {
        return this == INVALID;
    }

    public long length() {
        return end - start + 1;
    }

    /**
     * @return null when there is no usable Range header (serve the full body with 200),
     * INVALID when unsatisfiable (416), otherwise the clamped range.
     * Multi-range requests are ignored (full body), which RFC 9110 allows.
     */
    public static ByteRange parse(String header, long size) {
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
            if (a.isEmpty()) {
                if (b.isEmpty()) return null;
                long n = Long.parseLong(b);
                if (n <= 0 || size == 0) return INVALID;
                return new ByteRange(Math.max(0, size - n), size - 1);
            }
            long s = Long.parseLong(a);
            if (s < 0) return null;
            long e;
            if (b.isEmpty()) {
                e = size - 1;
            } else {
                e = Long.parseLong(b);
                if (e < s) return null;
            }
            if (s >= size) return INVALID;
            if (e >= size) e = size - 1;
            return new ByteRange(s, e);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
