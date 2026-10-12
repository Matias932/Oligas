package com.stroku.p2p;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;

/** Network helpers that do not depend on Android APIs. */
public final class NetUtil {
    private NetUtil() {}

    /**
     * Best LAN IPv4 of this device, or null. Wi-Fi first, then hotspot / ethernet, then any other
     * private address; cellular, VPN and virtual interfaces are skipped.
     */
    public static String lanAddress() {
        String best = null;
        int bestScore = -1;
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                String n = ni.getName().toLowerCase(Locale.ROOT);
                if (n.startsWith("rmnet") || n.startsWith("ccmni") || n.startsWith("pdp") || n.startsWith("tun")
                        || n.startsWith("dummy") || n.startsWith("p2p") || n.startsWith("v4-") || n.startsWith("ppp")) continue;
                int score = n.startsWith("wlan") ? 100 : (n.startsWith("swlan") || n.startsWith("ap")) ? 90
                        : n.startsWith("eth") ? 80 : 10;
                for (InetAddress a : Collections.list(ni.getInetAddresses())) {
                    if (a instanceof Inet4Address && a.isSiteLocalAddress() && score > bestScore) {
                        best = a.getHostAddress();
                        bestScore = score;
                    }
                }
            }
        } catch (IOException ignored) {
        }
        return best;
    }

    /** True if a TCP connection to host:port succeeds. */
    public static boolean tcpOpen(String host, int port, int timeoutMs) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** GET for local health checks only (callers pass fixed loopback URLs). Body capped at 64 KiB. */
    public static String httpGet(String host, int port, String path, int timeoutMs) throws IOException {
        HttpURLConnection c = (HttpURLConnection) new URL("http", host, port, path).openConnection();
        c.setInstanceFollowRedirects(false);
        c.setConnectTimeout(timeoutMs);
        c.setReadTimeout(timeoutMs);
        try {
            int code = c.getResponseCode();
            if (code != 200) throw new IOException("HTTP " + code);
            try (InputStream in = c.getInputStream()) {
                ByteArrayOutputStream bo = new ByteArrayOutputStream();
                byte[] b = new byte[4096];
                int n;
                while ((n = in.read(b)) >= 0) {
                    bo.write(b, 0, n);
                    if (bo.size() > 64 * 1024) throw new IOException("response too large");
                }
                return new String(bo.toByteArray(), StandardCharsets.UTF_8);
            }
        } finally {
            c.disconnect();
        }
    }

    /**
     * Best-effort identification of whatever already listens on a loopback port: sends a plain
     * GET /health and returns the status line, Server header and the start of the body. Android
     * does not let apps see which process owns a socket, so this is the only honest evidence.
     */
    public static String whoIsOn(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", port), 1000);
            s.setSoTimeout(1500);
            s.getOutputStream().write(("GET /health HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            s.getOutputStream().flush();
            byte[] buf = new byte[600];
            int got = 0, n;
            try {
                while (got < buf.length && (n = s.getInputStream().read(buf, got, buf.length - got)) > 0) got += n;
            } catch (IOException ignored) {
            }
            if (got == 0) return "algo acepta conexiones pero no responde HTTP";
            String txt = new String(buf, 0, got, StandardCharsets.UTF_8).replaceAll("[^\\x20-\\x7E\\r\\n]", "?");
            String[] lines = txt.split("\\r?\\n");
            StringBuilder sb = new StringBuilder(lines[0].trim());
            for (String l : lines) {
                if (l.toLowerCase(java.util.Locale.ROOT).startsWith("server:") || l.toLowerCase(java.util.Locale.ROOT).startsWith("x-powered-by:")) {
                    sb.append(" | ").append(l.trim());
                }
            }
            int body = txt.indexOf("\r\n\r\n");
            if (body >= 0 && body + 4 < txt.length()) {
                String b = txt.substring(body + 4).replaceAll("\\s+", " ").trim();
                if (!b.isEmpty()) sb.append(" | ").append(b.length() > 140 ? b.substring(0, 140) + "…" : b);
            }
            return sb.toString();
        } catch (IOException e) {
            return "no responde en 127.0.0.1 (puede estar enlazado solo a la IP de la red)";
        }
    }
}
