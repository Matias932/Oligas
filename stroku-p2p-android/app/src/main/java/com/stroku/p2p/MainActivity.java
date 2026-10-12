package com.stroku.p2p;

import android.Manifest;
import android.app.Activity;
import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.graphics.Typeface;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.PowerManager;
import android.provider.Settings;
import android.view.Gravity;
import android.view.WindowInsets;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import java.util.List;

public class MainActivity extends Activity {

    private static final int REQ_NOTIF = 11;

    private final Handler ui = new Handler(Looper.getMainLooper());
    private TextView status;
    private TextView address;
    private TextView report;
    private TextView logView;
    private Button startBtn;
    private Button stopBtn;
    private boolean askedNotif;

    private int dp(float value) {
        return (int) (value * getResources().getDisplayMetrics().density + 0.5f);
    }

    private TextView text(String value, int size) {
        TextView t = new TextView(this);
        t.setText(value);
        t.setTextSize(size);
        t.setTextColor(Color.WHITE);
        t.setGravity(Gravity.CENTER);
        return t;
    }

    private Button button(String label) {
        Button b = new Button(this);
        b.setText(label);
        b.setAllCaps(false);
        return b;
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setGravity(Gravity.CENTER_HORIZONTAL);
        root.setPadding(dp(24), dp(30), dp(24), dp(24));
        root.setBackgroundColor(Color.rgb(15, 18, 28));

        TextView title = text("STROKU", 32);
        title.setTypeface(null, Typeface.BOLD);
        root.addView(title);

        TextView subtitle = text("P2P SERVER", 18);
        subtitle.setTextColor(Color.rgb(80, 200, 160));
        root.addView(subtitle);

        status = text("● SERVIDOR DETENIDO", 17);
        status.setPadding(0, dp(28), 0, dp(14));
        root.addView(status);

        address = text("Dirección: todavía no detectada", 15);
        address.setTextIsSelectable(true);
        root.addView(address);

        Config defaults = new Config();
        TextView info = text("\nGateway (LAN): puerto " + defaults.gatewayPort
                + "\nMotor interno (solo este teléfono): puerto " + defaults.enginePort + "\n", 13);
        info.setTextColor(Color.rgb(170, 178, 195));
        root.addView(info);

        startBtn = button("INICIAR SERVIDOR");
        root.addView(startBtn);
        startBtn.setOnClickListener(v -> onStartClicked());

        stopBtn = button("DETENER SERVIDOR");
        root.addView(stopBtn);
        stopBtn.setOnClickListener(v -> onStopClicked());

        Button check = button("COMPROBAR ESTADO");
        root.addView(check);
        check.setOnClickListener(v -> onCheckClicked());

        report = text("", 12);
        report.setTypeface(Typeface.MONOSPACE);
        report.setGravity(Gravity.START);
        report.setPadding(0, dp(14), 0, dp(6));
        report.setTextIsSelectable(true);
        root.addView(report);

        Button battery = button("Evitar que Android pause el servidor");
        battery.setOnClickListener(v -> askBatteryExemption());
        root.addView(battery);

        logView = text("", 11);
        logView.setTypeface(Typeface.MONOSPACE);
        logView.setGravity(Gravity.START);
        logView.setTextColor(Color.rgb(140, 148, 165));
        logView.setPadding(0, dp(10), 0, 0);
        root.addView(logView);

        ScrollView scroll = new ScrollView(this);
        scroll.setBackgroundColor(Color.rgb(15, 18, 28));
        scroll.addView(root);
        scroll.setOnApplyWindowInsetsListener((v, insets) -> { // targetSdk 35 draws edge-to-edge
            int top, bottom;
            if (Build.VERSION.SDK_INT >= 30) {
                android.graphics.Insets b = insets.getInsets(WindowInsets.Type.systemBars() | WindowInsets.Type.displayCutout());
                top = b.top;
                bottom = b.bottom;
            } else {
                top = insets.getSystemWindowInsetTop();
                bottom = insets.getSystemWindowInsetBottom();
            }
            v.setPadding(0, top, 0, bottom);
            return insets;
        });
        setContentView(scroll);
    }

    @Override
    protected void onResume() {
        super.onResume();
        ui.post(tick);
    }

    @Override
    protected void onPause() {
        ui.removeCallbacks(tick);
        super.onPause();
    }

    private final Runnable tick = new Runnable() {
        @Override
        public void run() {
            refresh();
            ui.postDelayed(this, 1000);
        }
    };

    private void refresh() {
        ServerState.State s = ServerState.state();
        switch (s) {
            case RUNNING:
                status.setText("● SERVIDOR ACTIVO");
                status.setTextColor(Color.rgb(80, 200, 160));
                break;
            case STARTING:
                status.setText("● INICIANDO…");
                status.setTextColor(Color.rgb(240, 200, 80));
                break;
            case STOPPING:
                status.setText("● DETENIENDO…");
                status.setTextColor(Color.rgb(240, 200, 80));
                break;
            case ERROR:
                status.setText("● ERROR AL INICIAR");
                status.setTextColor(Color.rgb(240, 100, 100));
                break;
            default:
                status.setText("● SERVIDOR DETENIDO");
                status.setTextColor(Color.WHITE);
        }
        String url = ServerState.gatewayUrl();
        if (s == ServerState.State.RUNNING) {
            address.setText(url.isEmpty() ? "Dirección: sin IP LAN (¿Wi-Fi apagado?)" : "Dirección del gateway:\n" + url);
        } else if (s == ServerState.State.ERROR) {
            address.setText("Error: " + ServerState.lastError());
        } else {
            address.setText("Dirección: todavía no detectada");
        }
        boolean busy = s == ServerState.State.STARTING || s == ServerState.State.STOPPING;
        startBtn.setEnabled(!busy && s != ServerState.State.RUNNING);
        stopBtn.setEnabled(!busy && s != ServerState.State.STOPPED);
        List<String> lines = Logx.recent(10);
        logView.setText(String.join("\n", lines));
    }

    // ------------------------------------------------------------------ actions

    private void onStartClicked() {
        if (Build.VERSION.SDK_INT >= 33 && !askedNotif
                && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            askedNotif = true;
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, REQ_NOTIF);
            return;
        }
        doStart();
    }

    @Override
    public void onRequestPermissionsResult(int code, String[] perms, int[] results) {
        super.onRequestPermissionsResult(code, perms, results);
        if (code == REQ_NOTIF) {
            if (results.length == 0 || results[0] != PackageManager.PERMISSION_GRANTED) {
                Toast.makeText(this, "Sin permiso de notificaciones el servidor funciona igual, pero no verás el aviso.",
                        Toast.LENGTH_LONG).show();
            }
            doStart();
        }
    }

    private void doStart() {
        try {
            startForegroundService(new Intent(this, P2PService.class).setAction(P2PService.ACTION_START));
        } catch (RuntimeException e) {
            ServerState.setError(String.valueOf(e.getMessage()));
            ServerState.set(ServerState.State.ERROR);
            Toast.makeText(this, "No se pudo iniciar el servicio: " + e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }

    private void onStopClicked() {
        startService(new Intent(this, P2PService.class).setAction(P2PService.ACTION_STOP));
    }

    private void onCheckClicked() {
        report.setText("Comprobando…");
        new Thread(() -> {
            StatusProbe.Report r = StatusProbe.run(ServerState.config());
            String txt = StatusProbe.format(r, ServerState.state().name(), ServerState.lastError());
            runOnUiThread(() -> report.setText(txt));
        }, "status-probe").start();
    }

    private void askBatteryExemption() {
        PowerManager pm = (PowerManager) getSystemService(POWER_SERVICE);
        if (pm.isIgnoringBatteryOptimizations(getPackageName())) {
            Toast.makeText(this, "Android ya no restringe la batería de esta app.", Toast.LENGTH_SHORT).show();
            return;
        }
        try {
            startActivity(new Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                    Uri.parse("package:" + getPackageName())));
        } catch (ActivityNotFoundException e) {
            startActivity(new Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS));
        }
    }
}
