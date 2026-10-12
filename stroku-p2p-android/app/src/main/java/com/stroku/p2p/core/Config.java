package com.stroku.p2p.core;

import java.io.File;

/**
 * Configuración del servidor. Los valores por defecto son los del gateway Node.js anterior
 * (bloque 4 MiB, caché 512 MiB, 5 descargas paralelas, prefetch 8-16, salto 16).
 * Los campos son instancia para poder usar puertos y tamaños pequeños en las pruebas.
 */
public final class Config {
    public static final int DEFAULT_GATEWAY_PORT = 4001;
    public static final int DEFAULT_ENGINE_PORT = 3000;
    /** El motor local SOLO escucha en loopback. */
    public static final String ENGINE_HOST = "127.0.0.1";

    public int gatewayPort = DEFAULT_GATEWAY_PORT;
    public String gatewayBind = "0.0.0.0";
    public int enginePort = DEFAULT_ENGINE_PORT;

    public int blockSize = 4 * 1024 * 1024;
    public long maxCacheBytes = 512L * 1024 * 1024;
    public int parallelFetches = 5;
    public int prefetchMinBlocks = 8;
    public int prefetchMaxBlocks = 16;
    public int seekBoostBlocks = 16;

    /** Tiempo máximo para obtener los metadatos de un magnet. */
    public long resolveTimeoutMs = 90_000;
    /** Tiempo máximo esperando a que el torrent entregue un trozo concreto. */
    public long pieceTimeoutMs = 60_000;
    /** Conexiones simultáneas máximas aceptadas por el gateway. */
    public int maxClientConnections = 32;

    public File cacheDir;

    public Config(File cacheDir) {
        this.cacheDir = cacheDir;
    }
}
