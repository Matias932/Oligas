package com.stroku.p2p;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.net.wifi.WifiManager;
import android.os.Build;
import android.os.IBinder;
import android.os.PowerManager;
import android.util.Log;

import java.io.File;
import java.io.IOException;

/**
 * Foreground service that owns the real servers: libtorrent4j engine, the internal engine API on
 * 127.0.0.1:3000 and the LAN gateway on 0.0.0.0:4001. Start verifies both ports with real
 * connections; Stop closes every socket, the torrent session and the on-disk data.
 */
public class P2PService extends Service {
    public static final String ACTION_START = "com.stroku.p2p.action.START";
    public static final String ACTION_STOP = "com.stroku.p2p.action.STOP";
    private static final String TAG = "service";
    private static final String CHANNEL = "stroku_p2p_server";
    private static final int NOTIF_ID = 4001;

    private final Object lock = new Object();
    private PowerManager.WakeLock wakeLock;
    private WifiManager.WifiLock wifiLock;
    private LibtorrentBackend backend;
    private EngineServer engine;
    private Gateway gateway;
    private BlockCache cache;
    private volatile boolean refresherRun;

    @Override
    public void onCreate() {
        super.onCreate();
        Logx.setSink((level, tag, msg, t) -> {
            int prio = level == 'E' ? Log.ERROR : level == 'W' ? Log.WARN : Log.INFO;
            Log.println(prio, "Stroku/" + tag, msg + (t != null ? " " + Log.getStackTraceString(t) : ""));
        });
        NotificationChannel ch = new NotificationChannel(CHANNEL, "Servidor P2P", NotificationManager.IMPORTANCE_LOW);
        ch.setDescription("Muestra que el servidor P2P de Stroku está activo");
        getSystemService(NotificationManager.class).createNotificationChannel(ch);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        String action = intent == null ? null : intent.getAction();
        if (ACTION_STOP.equals(action)) {
            stopServer();
        } else {
            // Must be promoted to foreground right away (5 s rule), then boot in the background.
            startForegroundCompat(getString(R.string.notif_starting));
            startServer();
        }
        return START_NOT_STICKY;
    }

    private void startForegroundCompat(String text) {
        Notification n = buildNotification(text);
        if (Build.VERSION.SDK_INT >= 34) {
            startForeground(NOTIF_ID, n, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE);
        } else {
            startForeground(NOTIF_ID, n);
        }
    }

    private Notification buildNotification(String text) {
        PendingIntent open = PendingIntent.getActivity(this, 0, new Intent(this, MainActivity.class),
                PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
        Intent stop = new Intent(this, P2PService.class).setAction(ACTION_STOP);
        PendingIntent stopPi = PendingIntent.getService(this, 1, stop,
                PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
        return new Notification.Builder(this, CHANNEL)
                .setSmallIcon(R.drawable.ic_stat_server)
                .setContentTitle("Stroku P2P Server")
                .setContentText(text)
                .setOngoing(true)
                .setContentIntent(open)
                .addAction(new Notification.Action.Builder(null, getString(R.string.notif_stop), stopPi).build())
                .build();
    }

    private void updateNotification(String text) {
        getSystemService(NotificationManager.class).notify(NOTIF_ID, buildNotification(text));
    }

    // ------------------------------------------------------------------ start

    private void startServer() {
        synchronized (lock) {
            ServerState.State s = ServerState.state();
            if (s == ServerState.State.STARTING || s == ServerState.State.RUNNING) {
                Logx.i(TAG, "start ignored: already " + s);
                return;
            }
            ServerState.set(ServerState.State.STARTING);
        }
        ServerState.setError("");
        Thread t = new Thread(this::bootstrap, "p2p-bootstrap");
        t.start();
    }

    private void bootstrap() {
        Config cfg = new Config();
        cfg.cacheDir = new File(getCacheDir(), "blocks");
        File ext = getExternalFilesDir("torrents");
        cfg.dataDir = ext != null ? ext : new File(getFilesDir(), "torrents");
        ServerState.setConfig(cfg);
        try {
            Logx.i(TAG, "starting: engine " + cfg.engineHost + ":" + cfg.enginePort + ", gateway " + cfg.gatewayBind + ":" + cfg.gatewayPort);
            acquireLocks();
            LibtorrentBackend be = new LibtorrentBackend(cfg.dataDir, cfg.maxTorrents);
            be.start();
            backend = be;
            EngineServer en = new EngineServer(cfg, be);
            en.start();
            engine = en;
            BlockCache bc = new BlockCache(cfg.cacheDir, cfg.maxCache);
            cache = bc;
            Gateway gw = new Gateway(cfg, bc);
            gw.start();
            gateway = gw;

            StatusProbe.Report r = StatusProbe.run(cfg);
            if (!r.engineListening || !r.engineHealthOk) {
                throw new IOException("el motor no responde en 127.0.0.1:" + cfg.enginePort);
            }
            if (!r.gatewayListening) throw new IOException("el gateway no escucha en el puerto " + cfg.gatewayPort);
            if (!r.gatewayHealthOk) throw new IOException("el gateway no puede hablar con el motor");

            ServerState.setLanIp(r.lanIp);
            ServerState.set(ServerState.State.RUNNING);
            Logx.i(TAG, "RUNNING " + (r.lanIp.isEmpty() ? "(sin IP LAN)" : ServerState.gatewayUrl()));
            updateNotification(notificationText());
            startRefresher();
        } catch (Throwable t) {
            Logx.e(TAG, "start failed", t);
            String msg = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
            ServerState.setError(msg);
            teardown();
            ServerState.set(ServerState.State.ERROR);
            stopForeground(STOP_FOREGROUND_REMOVE);
            stopSelf();
        }
    }

    private String notificationText() {
        String url = ServerState.gatewayUrl();
        return url.isEmpty() ? getString(R.string.notif_no_lan) : getString(R.string.notif_running, url);
    }

    /** Keeps the LAN address (and the notification) current if Wi-Fi changes while running. */
    private void startRefresher() {
        refresherRun = true;
        Thread t = new Thread(() -> {
            while (refresherRun) {
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    return;
                }
                if (!refresherRun || ServerState.state() != ServerState.State.RUNNING) return;
                String ip = NetUtil.lanAddress();
                ip = ip == null ? "" : ip;
                if (!ip.equals(ServerState.lanIp())) {
                    ServerState.setLanIp(ip);
                    Logx.i(TAG, "LAN address changed: " + (ip.isEmpty() ? "none" : ip));
                    updateNotification(notificationText());
                }
            }
        }, "p2p-ip-refresh");
        t.setDaemon(true);
        t.start();
    }

    // ------------------------------------------------------------------ stop

    private void stopServer() {
        ServerState.State s = ServerState.state();
        if (s == ServerState.State.STOPPED || s == ServerState.State.ERROR) {
            stopForeground(STOP_FOREGROUND_REMOVE);
            stopSelf();
            return;
        }
        ServerState.set(ServerState.State.STOPPING);
        new Thread(() -> {
            teardown();
            ServerState.set(ServerState.State.STOPPED);
            ServerState.setLanIp("");
            Logx.i(TAG, "STOPPED");
            stopForeground(STOP_FOREGROUND_REMOVE);
            stopSelf();
        }, "p2p-shutdown").start();
    }

    /** Idempotent: closes sockets, the torrent session and temporary data in reverse start order. */
    private void teardown() {
        refresherRun = false;
        synchronized (lock) {
            try {
                if (gateway != null) gateway.close();
            } catch (Throwable t) {
                Logx.e(TAG, "gateway close", t);
            }
            try {
                if (engine != null) engine.close();
            } catch (Throwable t) {
                Logx.e(TAG, "engine close", t);
            }
            try {
                if (backend != null) backend.close();
            } catch (Throwable t) {
                Logx.e(TAG, "backend close", t);
            }
            try {
                if (cache != null) cache.clearAll();
            } catch (Throwable t) {
                Logx.e(TAG, "cache clear", t);
            }
            gateway = null;
            engine = null;
            backend = null;
            cache = null;
            releaseLocks();
        }
    }

    private void acquireLocks() {
        PowerManager pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "stroku:p2p");
        wakeLock.setReferenceCounted(false);
        wakeLock.acquire();
        WifiManager wm = (WifiManager) getApplicationContext().getSystemService(Context.WIFI_SERVICE);
        int mode = Build.VERSION.SDK_INT >= 29 ? WifiManager.WIFI_MODE_FULL_LOW_LATENCY : WifiManager.WIFI_MODE_FULL_HIGH_PERF;
        wifiLock = wm.createWifiLock(mode, "stroku:p2p");
        wifiLock.setReferenceCounted(false);
        wifiLock.acquire();
    }

    private void releaseLocks() {
        try {
            if (wifiLock != null && wifiLock.isHeld()) wifiLock.release();
        } catch (Throwable ignored) {
        }
        try {
            if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        } catch (Throwable ignored) {
        }
        wifiLock = null;
        wakeLock = null;
    }

    @Override
    public void onDestroy() {
        ServerState.State s = ServerState.state();
        if (s == ServerState.State.RUNNING || s == ServerState.State.STARTING || s == ServerState.State.STOPPING) {
            teardown();
            ServerState.set(ServerState.State.STOPPED);
        }
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }
}
