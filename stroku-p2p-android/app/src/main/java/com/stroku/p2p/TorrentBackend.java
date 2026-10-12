package com.stroku.p2p;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Narrow interface between the HTTP layers and the real torrent engine. The only implementation
 * that ships is LibtorrentBackend (libtorrent4j); tests use an in-memory fake.
 */
public interface TorrentBackend extends Closeable {

    final class FileMeta {
        public final int index;
        public final String path;
        public final long size;

        public FileMeta(int index, String path, long size) {
            this.index = index;
            this.path = path;
            this.size = size;
        }
    }

    final class Meta {
        public final String infoHash;
        public final String name;
        public final long pieceLength;
        public final List<FileMeta> files;
        public final int bestFile;

        public Meta(String infoHash, String name, long pieceLength, List<FileMeta> files, int bestFile) {
            this.infoHash = infoHash;
            this.name = name;
            this.pieceLength = pieceLength;
            this.files = files;
            this.bestFile = bestFile;
        }
    }

    /** Adds the (sanitized) magnet and blocks until metadata is known or the timeout expires. */
    Meta addMagnet(String sanitizedMagnet, long timeoutMs) throws IOException;

    /** Metadata of an already added torrent, or null. */
    Meta meta(String infoHash);

    /** Hints the engine to fetch [offset, offset+length) of the file as soon as possible. */
    void prioritize(String infoHash, int fileIndex, long offset, long length);

    /** Contiguous bytes (at most maxLen) that are fully downloaded and verified starting at offset. */
    long availableFrom(String infoHash, int fileIndex, long offset, long maxLen);

    /** Reads bytes previously reported by availableFrom. */
    int read(String infoHash, int fileIndex, long offset, byte[] buf, int off, int len) throws IOException;

    /** Live engine statistics for the health endpoints. */
    Map<String, Object> status();

    @Override
    void close();
}
