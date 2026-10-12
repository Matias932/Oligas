# Stroku P2P Server (Android)

    Roku (Stroku) -> gateway 0.0.0.0:4001 -> API interna 127.0.0.1:3000 -> libtorrent4j -> torrent

- **Motor torrent:** libtorrent4j (libtorrent nativo dentro del APK, solo arm64-v8a). No usa Termux ni procesos externos.
- **API interna :3000:** código propio de esta app (`EngineServer`). **No es KiraLT**: KiraLT no está en el repositorio
  ni se pudo identificar, así que no se integró. Solo escucha en loopback y exige un token generado en cada arranque.
- **Gateway :4001:** `GET|HEAD /play?magnet=...` (HTTP Range, 206) y `GET /health`. Caché en bloques de 4 MiB (máx. 512 MiB),
  prefetch 8-16 bloques, 5 descargas paralelas, 16 bloques de refuerzo tras un salto (valores de la versión anterior).
- **Servicio:** servicio en primer plano (`specialUse`) con notificación, wake lock y Wi-Fi lock.

## Uso

1. Instala el APK (permite "instalar apps desconocidas" para tu navegador/gestor de archivos).
2. Abre la app, pulsa **INICIAR SERVIDOR** y acepta las notificaciones.
3. Anota la dirección que muestra, p. ej. `http://192.168.2.120:4001`, y ponla en los ajustes de Stroku (ver `ROKU-INTEGRATION.md`).
4. Toca **Evitar que Android pause el servidor** y permite la excepción de batería (importante en Samsung).
5. **COMPROBAR ESTADO** hace comprobaciones reales (TCP + `/health`) y muestra puertos, IP, caché y errores.
6. **DETENER SERVIDOR** cierra los sockets y la sesión torrent y borra lo descargado y la caché.

## Seguridad

- No es un proxy: solo `/play` y `/health`; el único destino de salida es `127.0.0.1:<puerto del motor>`; sin redirecciones.
- Solo acepta magnets (`btih` de 40 hex o 32 base32). Se descartan `ws`, `xs`, `as` y trackers a IPs privadas/localhost.
  No se aceptan URLs `.torrent`, porque obligarían al teléfono a descargar de destinos arbitrarios.
- Solo clientes de red local (privada, link-local, loopback). Límite de 48 conexiones y cabeceras acotadas.
- Un bloque de caché solo es visible tras escribirse completo, con la longitud exacta y un renombrado atómico.

## Pruebas

`selftest/GatewaySelfTest.java` (se ejecuta en GitHub Actions antes de compilar): 38 comprobaciones HTTP reales contra un motor
falso en memoria. **No** prueban libtorrent4j ni Android.

## Limitaciones conocidas

- libtorrent4j nunca se ha ejecutado en un dispositivo por mí: que descargue y transmita de verdad debe confirmarse en el S26.
- Se desconoce si la versión `2.1.0-39` de libtorrent4j existe en Maven; se cambia en `gradle.properties` si falla la compilación.
- Si Android mata el proceso en segundo plano, el servidor se detiene (no se reinicia solo).
- Cada reinicio del servidor borra lo descargado y la caché.
- Sin autodetección de IP en Roku (ver `ROKU-INTEGRATION.md`).
