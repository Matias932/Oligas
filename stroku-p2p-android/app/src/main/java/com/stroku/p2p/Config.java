package com.stroku.p2p;

import java.io.File;
import java.security.SecureRandom;

/**
 * Runtime configuration. The defaults come from the previous gateway reference values;
 * tests override them (small blocks, ephemeral ports).
 */
public final class Config {
    // Network
    public String gatewayBind = "0.0.0.0";
    public int gatewayPort = 4001;
    public String engineHost = "127.0.0.1"; // fixed: the only upstream the gateway may talk to
    /** Port the Roku app puts in its KiraLT-style URLs; only validated, never connected to. */
    public final int contractEnginePort = 3000;
    public int enginePort = 3000;

    // Gateway block cache (reference values from the earlier Node gateway)
    public int blockSize = 4 * 1024 * 1024;
    public long maxCache = 512L * 1024 * 1024;
    public int parallelFetches = 5;
    public int prefetchMinBlocks = 8;
    public int prefetchMaxBlocks = 16;
    public int seekBoostBlocks = 16;

    // Limits / timeouts
    public int maxConnections = 48;
    public long gatewayIdleMs = 150_000;
    public long engineIdleMs = 120_000;
    public long metadataTimeoutMs = 60_000;
    public long pieceWaitTimeoutMs = 45_000;
    public int maxTorrents = 2;
    public long engineWindowBytes = 24L * 1024 * 1024;

    // Storage
    public File cacheDir;
    public File dataDir;

    /** Shared secret between gateway and engine, regenerated at every start. */
    public final String engineToken = newToken();

    private static String newToken() {
        byte[] b = new byte[18];
        new SecureRandom().nextBytes(b);
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x & 0xff));
        return sb.toString();
    }
}
