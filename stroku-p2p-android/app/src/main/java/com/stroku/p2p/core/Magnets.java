package com.stroku.p2p.core;

import java.util.Locale;

/**
 * Validación estricta de enlaces magnet. Solo se conservan los parámetros xt, tr y x.pe;
 * se descartan dn, ws, xs, as y cualquier otro para que un magnet no pueda hacer que el teléfono
 * pida URLs arbitrarias (web seeds, fuentes exactas, etc.).
 */
public final class Magnets {
    private Magnets() {}

    public static final class Parsed {
        public final String infoHash; // 40 hex en minúsculas
        public final String magnet;   // magnet reconstruido y filtrado

        Parsed(String infoHash, String magnet) {
            this.infoHash = infoHash;
            this.magnet = magnet;
        }
    }

    private static final String PREFIX = "magnet:?";
    private static final int MAX_LEN = 8192;
    private static final int MAX_TRACKERS = 20;

    /** @return null si no es un magnet válido y seguro. */
    public static Parsed parse(String raw) {
        if (raw == null || raw.length() > MAX_LEN || !raw.regionMatches(true, 0, PREFIX, 0, PREFIX.length())) {
            return null;
        }
        String hash = null;
        StringBuilder out = new StringBuilder(PREFIX);
        int trackers = 0;
        boolean first = true;
        for (String part : raw.substring(PREFIX.length()).split("&")) {
            if (part.isEmpty()) continue;
            int eq = part.indexOf('=');
            if (eq <= 0) continue;
            String key = part.substring(0, eq).toLowerCase(Locale.ROOT);
            String val = part.substring(eq + 1);
            boolean keep = false;
            if (key.equals("xt")) {
                if (!plainAscii(val)) return null;
                String h = hashFromXt(val);
                if (h == null) return null;
                if (hash != null && !hash.equals(h)) return null; // dos hashes distintos: ambiguo
                if (hash == null) {
                    hash = h;
                    keep = true;
                }
            } else if (key.equals("x.pe")) {
                keep = plainAscii(val) && val.length() < 128;
            } else if (key.equals("tr")) {
                if (trackers < MAX_TRACKERS && val.length() < 512 && plainAscii(val)) {
                    trackers++;
                    keep = true;
                }
            }
            if (keep) {
                if (!first) out.append('&');
                out.append(key).append('=').append(val);
                first = false;
            }
        }
        if (hash == null) return null;
        return new Parsed(hash, out.toString());
    }

    /** Valida un info-hash escrito directamente (40 hex). */
    public static String normalizeHash(String s) {
        if (s == null || s.length() != 40) return null;
        String l = s.toLowerCase(Locale.ROOT);
        for (int i = 0; i < 40; i++) {
            char c = l.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return null;
        }
        return l;
    }

    private static boolean plainAscii(String v) {
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c < 0x21 || c > 0x7e) return false;
        }
        return true;
    }

    private static String hashFromXt(String val) {
        String p = "urn:btih:";
        if (!val.regionMatches(true, 0, p, 0, p.length())) return null;
        String h = val.substring(p.length());
        if (h.length() == 40) return normalizeHash(h);
        if (h.length() == 32) return base32ToHex(h);
        return null;
    }

    private static String base32ToHex(String s) {
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        String u = s.toUpperCase(Locale.ROOT);
        byte[] out = new byte[20];
        int buffer = 0, bits = 0, idx = 0;
        for (int i = 0; i < u.length(); i++) {
            int v = alphabet.indexOf(u.charAt(i));
            if (v < 0) return null;
            buffer = ((buffer << 5) | v) & 0xffff;
            bits += 5;
            if (bits >= 8) {
                bits -= 8;
                if (idx >= 20) return null;
                out[idx++] = (byte) ((buffer >> bits) & 0xff);
            }
        }
        if (idx != 20) return null;
        StringBuilder sb = new StringBuilder(40);
        for (byte b : out) sb.append(String.format("%02x", b & 0xff));
        return sb.toString();
    }
}
