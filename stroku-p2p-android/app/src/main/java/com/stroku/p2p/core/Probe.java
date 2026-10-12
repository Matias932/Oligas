package com.stroku.p2p.core;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;

/** Petición HTTP GET real, con tiempo medido, para comprobar el estado de los servicios. */
public final class Probe {
    public final int code;
    public final String body;
    public final long millis;
    public final String error;

    private Probe(int code, String body, long millis, String error) {
        this.code = code;
        this.body = body;
        this.millis = millis;
        this.error = error;
    }

    public boolean ok() {
        return error == null && code == 200;
    }

    public static Probe get(String url, int timeoutMs) {
        long t0 = System.nanoTime();
        HttpURLConnection c = null;
        try {
            c = (HttpURLConnection) new URL(url).openConnection(Proxy.NO_PROXY);
            c.setInstanceFollowRedirects(false);
            c.setConnectTimeout(timeoutMs);
            c.setReadTimeout(timeoutMs);
            c.setUseCaches(false);
            int code = c.getResponseCode();
            InputStream in = code >= 400 ? c.getErrorStream() : c.getInputStream();
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            if (in != null) {
                byte[] b = new byte[2048];
                int n;
                while ((n = in.read(b)) > 0 && bo.size() < 16384) bo.write(b, 0, n);
                in.close();
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            return new Probe(code, bo.toString("UTF-8"), ms, null);
        } catch (Exception e) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            return new Probe(-1, "", ms, e.getClass().getSimpleName() + ": " + e.getMessage());
        } finally {
            if (c != null) c.disconnect();
        }
    }
}
