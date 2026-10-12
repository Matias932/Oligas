package com.stroku.p2p;

/** Process-wide server state shared by the service and the UI. */
public final class ServerState {
    public enum State { STOPPED, STARTING, RUNNING, STOPPING, ERROR }

    private static volatile State state = State.STOPPED;
    private static volatile String lastError = "";
    private static volatile String lanIp = "";
    private static volatile Config config;

    private ServerState() {}

    public static State state() { return state; }
    public static String lastError() { return lastError; }
    public static String lanIp() { return lanIp; }
    public static Config config() { return config; }

    public static void set(State s) { state = s; }
    public static void setError(String e) { lastError = e == null ? "" : e; }
    public static void setLanIp(String ip) { lanIp = ip == null ? "" : ip; }
    public static void setConfig(Config c) { config = c; }

    public static String gatewayUrl() {
        Config c = config;
        String ip = lanIp;
        if (c == null || ip.isEmpty()) return "";
        return "http://" + ip + ":" + c.gatewayPort;
    }
}
