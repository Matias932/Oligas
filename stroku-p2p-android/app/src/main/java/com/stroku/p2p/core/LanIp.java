package com.stroku.p2p.core;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

/** Detecta la IPv4 de la red local (Wi-Fi o punto de acceso) sin fijar nunca una IP. */
public final class LanIp {
    public final String ip;
    public final String iface;

    private LanIp(String ip, String iface) {
        this.ip = ip;
        this.iface = iface;
    }

    /** @return la IP LAN preferida o null si el teléfono no está en una red local. */
    public static LanIp detect() {
        try {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            if (en == null) return null;
            LanIp best = null;
            int bestScore = -1;
            List<NetworkInterface> list = Collections.list(en);
            for (NetworkInterface ni : list) {
                if (!ni.isUp() || ni.isLoopback() || ni.isPointToPoint()) continue;
                String name = ni.getName().toLowerCase(Locale.ROOT);
                if (name.startsWith("rmnet") || name.startsWith("dummy") || name.startsWith("tun")
                        || name.startsWith("p2p") || name.startsWith("v4-") || name.startsWith("ccmni")) {
                    continue; // datos móviles / túneles: no son la LAN
                }
                int score = name.startsWith("wlan") ? 3 : (name.startsWith("swlan") || name.startsWith("ap")) ? 2
                        : name.startsWith("eth") ? 2 : 1;
                List<InetAddress> addrs = new ArrayList<>(Collections.list(ni.getInetAddresses()));
                for (InetAddress a : addrs) {
                    if (a instanceof Inet4Address && a.isSiteLocalAddress() && score > bestScore) {
                        best = new LanIp(a.getHostAddress(), ni.getName());
                        bestScore = score;
                    }
                }
            }
            return best;
        } catch (Exception e) {
            return null;
        }
    }
}
