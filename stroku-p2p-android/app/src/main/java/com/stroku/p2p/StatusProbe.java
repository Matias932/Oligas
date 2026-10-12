package com.stroku.p2p;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Real checks behind "COMPROBAR ESTADO": TCP connects and HTTP health calls, not text changes. */
public final class StatusProbe {
    private StatusProbe() {}

    public static final class Report {
        public boolean engineListening, gatewayListening, engineHealthOk, gatewayHealthOk;
        public String lanIp = "";
        public String gatewayUrl = "";
        public String gatewayHealth = "";
        public String engineHealth = "";
        public String error = "";
        public int enginePort, gatewayPort;
        public long cacheBytes = -1, cacheMax = -1;
        public long torrents = -1, peers = -1, downRate = -1;
        public String engineName = "";
    }

    public static Report run(Config cfg) {
        Report r = new Report();
        r.lanIp = NetUtil.lanAddress();
        if (r.lanIp == null) r.lanIp = "";
        if (cfg == null) {
            Config d = new Config();
            r.enginePort = d.enginePort;
            r.gatewayPort = d.gatewayPort;
            r.error = "el servidor no se ha iniciado";
            if (NetUtil.tcpOpen("127.0.0.1", d.gatewayPort, 800)) {
                r.error += " · AVISO: el puerto " + d.gatewayPort + " ya lo usa otro programa: " + NetUtil.whoIsOn(d.gatewayPort);
            }
            if (NetUtil.tcpOpen("127.0.0.1", d.enginePort, 800)) {
                r.error += " · AVISO: el puerto " + d.enginePort + " ya lo usa otro programa: " + NetUtil.whoIsOn(d.enginePort);
            }
            return r;
        }
        r.enginePort = cfg.enginePort;
        r.gatewayPort = cfg.gatewayPort;
        r.gatewayUrl = r.lanIp.isEmpty() ? "" : "http://" + r.lanIp + ":" + cfg.gatewayPort;
        r.engineListening = NetUtil.tcpOpen("127.0.0.1", cfg.enginePort, 1500);
        r.gatewayListening = NetUtil.tcpOpen("127.0.0.1", cfg.gatewayPort, 1500);
        StringBuilder err = new StringBuilder();
        try {
            r.engineHealth = NetUtil.httpGet("127.0.0.1", cfg.enginePort, "/health", 2500);
            r.engineHealthOk = r.engineHealth.contains("\"ok\":true");
        } catch (Exception e) {
            err.append("motor: ").append(e.getMessage()).append("  ");
        }
        try {
            r.gatewayHealth = NetUtil.httpGet("127.0.0.1", cfg.gatewayPort, "/health", 3500);
            r.gatewayHealthOk = r.gatewayHealth.contains("\"ok\":true") && r.gatewayHealth.contains("\"engineOk\":true");
            r.cacheBytes = num(r.gatewayHealth, "\"bytes\":(\\d+)");
            r.cacheMax = num(r.gatewayHealth, "\"max\":(\\d+)");
        } catch (Exception e) {
            err.append("gateway: ").append(e.getMessage()).append("  ");
        }
        r.torrents = num(r.engineHealth, "\"torrents\":(\\d+)");
        r.peers = num(r.engineHealth, "\"peers\":(\\d+)");
        r.downRate = num(r.engineHealth, "\"downloadRate\":(\\d+)");
        Matcher m = Pattern.compile("\"engine\":\"([^\"]*)\"").matcher(r.engineHealth);
        if (m.find()) r.engineName = m.group(1);
        r.error = err.toString().trim();
        return r;
    }

    private static long num(String s, String regex) {
        Matcher m = Pattern.compile(regex).matcher(s == null ? "" : s);
        return m.find() ? Long.parseLong(m.group(1)) : -1;
    }

    private static String ok(boolean b) {
        return b ? "OK" : "NO";
    }

    public static String format(Report r, String stateName, String lastError) {
        StringBuilder sb = new StringBuilder();
        sb.append("Estado del servicio: ").append(stateName).append('\n');
        sb.append("Motor torrent (").append(r.engineName.isEmpty() ? "libtorrent4j" : r.engineName).append("): ")
                .append(ok(r.engineHealthOk)).append(" · 127.0.0.1:").append(r.enginePort)
                .append(r.engineListening ? " escuchando" : " sin respuesta").append('\n');
        sb.append("API interna del motor :").append(r.enginePort).append(" (no es KiraLT): ")
                .append(ok(r.engineListening && r.engineHealthOk)).append('\n');
        sb.append("Gateway: ").append(ok(r.gatewayHealthOk)).append(" · 0.0.0.0:").append(r.gatewayPort)
                .append(r.gatewayListening ? " escuchando" : " sin respuesta").append('\n');
        sb.append("IP LAN: ").append(r.lanIp.isEmpty() ? "no detectada (¿Wi-Fi apagado?)" : r.lanIp).append('\n');
        if (!r.gatewayUrl.isEmpty()) sb.append("Dirección: ").append(r.gatewayUrl).append('\n');
        if (r.torrents >= 0) {
            sb.append("Torrents activos: ").append(r.torrents);
            if (r.peers >= 0) sb.append(" · peers: ").append(r.peers);
            if (r.downRate >= 0) sb.append(" · bajada: ").append(r.downRate / 1024).append(" KiB/s");
            sb.append('\n');
        }
        if (r.cacheBytes >= 0) {
            sb.append("Caché: ").append(r.cacheBytes / (1024 * 1024)).append(" / ")
                    .append(r.cacheMax / (1024 * 1024)).append(" MiB\n");
        }
        if (!r.error.isEmpty()) sb.append("Errores de comprobación: ").append(r.error).append('\n');
        if (lastError != null && !lastError.isEmpty()) sb.append("Último error de arranque: ").append(lastError).append('\n');
        return sb.toString().trim();
    }
}
