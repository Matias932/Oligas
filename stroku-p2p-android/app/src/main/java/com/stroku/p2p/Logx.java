package com.stroku.p2p;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/** Tiny logger: pluggable sink (android.util.Log in the service) plus an in-memory ring for the UI. */
public final class Logx {
    public interface Sink {
        void log(char level, String tag, String msg, Throwable t);
    }

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final ArrayDeque<String> RING = new ArrayDeque<>();
    private static volatile Sink sink = (l, tag, m, t) ->
            System.err.println(l + "/" + tag + ": " + m + (t != null ? " [" + t + "]" : ""));

    private Logx() {}

    public static void setSink(Sink s) {
        sink = s != null ? s : sink;
    }

    public static void i(String tag, String msg) { emit('I', tag, msg, null); }
    public static void w(String tag, String msg) { emit('W', tag, msg, null); }
    public static void e(String tag, String msg, Throwable t) { emit('E', tag, msg, t); }

    private static void emit(char level, String tag, String msg, Throwable t) {
        String line = LocalTime.now().format(FMT) + " " + level + " " + tag + ": " + msg
                + (t != null ? " [" + t.getClass().getSimpleName() + ": " + t.getMessage() + "]" : "");
        synchronized (RING) {
            RING.addLast(line);
            while (RING.size() > 300) RING.removeFirst();
        }
        try {
            sink.log(level, tag, msg, t);
        } catch (Throwable ignored) {
        }
    }

    public static List<String> recent(int n) {
        synchronized (RING) {
            List<String> all = new ArrayList<>(RING);
            return all.subList(Math.max(0, all.size() - n), all.size());
        }
    }
}
