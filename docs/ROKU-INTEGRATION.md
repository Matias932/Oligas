# Conexión con el canal Stroku (Roku)

El Roku **no necesita cambios de código**. El APK implementa el contrato que Stroku ya usa:

- `GET /stream?url=<URL KiraLT codificada>`  (Range, HEAD)
- `GET /probe?url=...`  y  `GET /warm?url=...`  (JSON)
- La URL interna tiene la forma `http://127.0.0.1:3000/stream/<magnet codificado>[?file=<nombre>|?fileIndex=<n, base 1>]`.
  El gateway **solo valida** esa URL y extrae el magnet; nunca se conecta a lo que diga la URL.

## Configurar
1. En el teléfono: abre la app, pulsa INICIAR SERVIDOR y copia la dirección `http://IP:4001`.
2. En Stroku (Roku): configura el gateway P2P con esa dirección (ajustes / clave `p2pGateway`).
   La IP del teléfono puede cambiar con el router; la app la muestra cada vez.

## Límites conocidos
- `lang`, `quality` y `codec` se aceptan y se devuelven en `/probe` y `/warm`, pero no cambian la selección de archivo.
- `/cache` no está implementado (Stroku no lo usa).
- El puerto 3000 es el motor propio de la app (no KiraLT). Si otra app ocupa el 3000, el motor usa otro puerto
  interno y el gateway sigue aceptando las URL `:3000` del Roku.
- Sin probar en un dispositivo real salvo lo que reporte el usuario.
