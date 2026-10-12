package com.stroku.p2p;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Validates magnet links and rebuilds a sanitized one. Only the info-hash, a display name and a
 * bounded list of udp/http(s) trackers survive: web seeds (ws), exact/acceptable sources (xs/as)
 * and anything else are dropped, so a magnet cannot make the phone fetch arbitrary URLs.
 */
public final class MagnetUtil {
    public static final int MAX_LEN = 4096;
    private static final int MAX_TRACKERS = 16;
    private static final Pattern HEX40 = Pattern.compile("^[0-9a-fA-F]{40}$");
    private static final Pattern B32 = Pattern.compile("^[A-Za-z2-7]{32}$");
    private static final Pattern IP_LITERAL = Pattern.compile("^[0-9.]+$|^\\[?[0-9a-fA-F:]+\\]?$");

    private MagnetUtil() {}

    public static final class Parsed {
        public final String infoHash;   // 40 lowercase hex chars
        public final String sanitized;  // rebuilt magnet URI

        Parsed(String infoHash, String sanitized) {
            this.infoHash = infoHash;
            this.sanitized = sanitized;
        }
    }

    public static boolean isHash(String s) {
        return s != null && s.length() == 40 && HEX40.matcher(s).matches();
    }

    public static Parsed parse(String raw) {
        if (raw == null || raw.isEmpty()) throw new IllegalArgumentException("empty magnet");
        if (raw.length() > MAX_LEN) throw new IllegalArgumentException("magnet too long");
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c <= 0x20 || c == 0x7f) throw new IllegalArgumentException("illegal character in magnet");
        }
        if (!raw.regionMatches(true, 0, "magnet:?", 0, 8)) {
            throw new IllegalArgumentException("not a magnet URI");
        }
        String hash = null;
        String dn = null;
        List<String> trackers = new ArrayList<>();
        for (String part : raw.substring(8).split("&")) {
            int eq = part.indexOf('=');
            if (eq <= 0) continue;
            String key = part.substring(0, eq).toLowerCase(Locale.ROOT);
            String val;
            try {
                val = URLDecoder.decode(part.substring(eq + 1), "UTF-8");
            } catch (UnsupportedEncodingException | IllegalArgumentException e) {
                continue;
            }
            switch (key) {
                case "xt":
                    if (hash == null && val.regionMatches(true, 0, "urn:btih:", 0, 9)) {
                        hash = toHex(val.substring(9));
                    }
                    break;
                case "dn":
                    if (dn == null) dn = clean(val, 200);
                    break;
                case "tr":
                    if (trackers.size() < MAX_TRACKERS && trackerOk(val)) trackers.add(val);
                    break;
                default:
                    break; // ws, xs, as, kt, ... dropped on purpose
            }
        }
        if (hash == null) throw new IllegalArgumentException("magnet has no valid btih info-hash");
        StringBuilder sb = new StringBuilder("magnet:?xt=urn:btih:").append(hash);
        try {
            if (dn != null && !dn.isEmpty()) sb.append("&dn=").append(URLEncoder.encode(dn, "UTF-8"));
            for (String t : trackers) sb.append("&tr=").append(URLEncoder.encode(t, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
        return new Parsed(hash, sb.toString());
    }

    private static String clean(String s, int max) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length() && sb.length() < max; i++) {
            char c = s.charAt(i);
            if (c >= 0x20 && c != 0x7f) sb.append(c);
        }
        return sb.toString().trim();
    }

    private static boolean trackerOk(String t) {
        if (t.length() > 512) return false;
        String low = t.toLowerCase(Locale.ROOT);
        if (!(low.startsWith("udp://") || low.startsWith("http://") || low.startsWith("https://"))) return false;
        try {
            URI u = new URI(t);
            String host = u.getHost();
            if (host == null || host.isEmpty()) return false;
            if (host.equalsIgnoreCase("localhost") || host.endsWith(".local")) return false;
            if (IP_LITERAL.matcher(host).matches()) {
                InetAddress a = InetAddress.getByName(host); // literal only, no DNS lookup
                if (a.isAnyLocalAddress() || a.isLoopbackAddress() || a.isSiteLocalAddress()
                        || a.isLinkLocalAddress() || a.isMulticastAddress()) return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String toHex(String v) {
        if (HEX40.matcher(v).matches()) return v.toLowerCase(Locale.ROOT);
        if (B32.matcher(v).matches()) {
            String s = v.toUpperCase(Locale.ROOT);
            byte[] out = new byte[20];
            int buffer = 0, bits = 0, idx = 0;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                int val = c >= 'A' && c <= 'Z' ? c - 'A' : c - '2' + 26;
                buffer = (buffer << 5) | val;
                bits += 5;
                if (bits >= 8) {
                    out[idx++] = (byte) ((buffer >> (bits - 8)) & 0xff);
                    bits -= 8;
                }
            }
            StringBuilder sb = new StringBuilder();
            for (byte b : out) sb.append(String.format("%02x", b & 0xff));
            return sb.toString();
        }
        return null;
    }
}
