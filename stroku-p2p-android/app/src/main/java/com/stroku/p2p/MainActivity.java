package com.stroku.p2p;

import android.app.Activity;
import android.os.Bundle;
import android.graphics.Color;
import android.graphics.Typeface;
import android.view.Gravity;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Button;
import android.widget.ScrollView;
import android.widget.Toast;

public class MainActivity extends Activity {

    private TextView status;
    private TextView address;

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
        status.setPadding(0, dp(35), 0, dp(20));
        root.addView(status);

        address = text("Dirección: todavía no detectada", 15);
        root.addView(address);

        TextView info = text(
            "\nTu gateway utiliza el puerto 4001.\n" +
            "KiraLT utiliza el puerto 3000.\n\n" +
            "Esta pantalla todavía no inicia los servicios.",
            14
        );
        root.addView(info);

        Button start = button("INICIAR SERVIDOR");
        root.addView(start);
        start.setOnClickListener(v ->
            Toast.makeText(this,
                "La conexión con KiraLT y el gateway aún debe configurarse.",
                Toast.LENGTH_LONG).show()
        );

        Button stop = button("DETENER SERVIDOR");
        root.addView(stop);
        stop.setOnClickListener(v ->
            Toast.makeText(this,
                "Todavía no hay servicios controlados por la aplicación.",
                Toast.LENGTH_LONG).show()
        );

        Button check = button("COMPROBAR ESTADO");
        root.addView(check);
        check.setOnClickListener(v ->
            Toast.makeText(this,
                "Aún falta conectar la comprobación del gateway.",
                Toast.LENGTH_LONG).show()
        );

        ScrollView scroll = new ScrollView(this);
        scroll.addView(root);
        setContentView(scroll);
    }
}
