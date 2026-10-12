package com.stroku.p2p.core;

import java.io.IOException;

/** Lo que el servidor local necesita de un motor torrent. Lo implementa engine.TorrentEngine. */
public interface TorrentBackend {

    /** El motor no consiguió metadatos o trozos a tiempo (se traduce en HTTP 504). */
    class TimeoutIOException extends IOException {
        public TimeoutIOException(String m) {
            super(m);
        }
    }

    final class Meta {
        public final String hash;
        public final int fileIndex;
        public final String name;
        public final long size;
        public final String mime;

        public Meta(String hash, int fileIndex, String name, long size, String mime) {
            this.hash = hash;
            this.fileIndex = fileIndex;
            this.name = name;
            this.size = size;
            this.mime = mime;
        }
    }

    /**
     * Añade el torrent si hace falta, espera a los metadatos y elige el archivo.
     *
     * @param magnet     magnet ya validado, o null si el torrent ya se añadió antes
     * @param fileIndex  índice del archivo o -1 para elegir el vídeo más grande
     */
    Meta resolve(String hash, String magnet, int fileIndex) throws IOException;

    /**
     * Lee bytes del archivo esperando a que los trozos estén descargados.
     *
     * @return bytes leídos (>0), o -1 si offset está al final del archivo
     */
    int read(Meta meta, long offset, byte[] buf, int off, int len) throws IOException;

    /** Estado real del motor como objeto JSON (para /health y la pantalla de estado). */
    String statusJson();

    void close();
}
