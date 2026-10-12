# Integración con Stroku (Roku)

El código de Stroku para Roku **no está en este repositorio**, así que no se ha modificado ni probado nada del lado Roku.
Esto es lo que hay que cambiar allí. Solo afecta a enlaces magnet; el reproductor original y Live TV no se tocan.

## URL que debe generar Stroku

    http://<IP-del-teléfono>:4001/play?magnet=<magnet URL-encoded>

El gateway acepta también el magnet sin codificar, pero conviene codificarlo.

## Cambio mínimo en `P2PPlaybackUrl` (basado en el fragmento que compartiste; sin probar)

```brightscript
function P2PPlaybackUrl(stream as object, contentType as string) as string
    sourceUrl = DirectStreamUrl(stream)

    if sourceUrl = "" then return ""
    if contentType = "live" then return sourceUrl          ' Live TV: sin cambios
    if not IsMagnetUrl(sourceUrl) then return sourceUrl     ' HTTP/HTTPS: reproductor original

    base = P2PGatewayBase()                                 ' p. ej. "http://192.168.2.120:4001"
    if base = "" then return ""                             ' sin gateway configurado: no se inventa nada

    xfer = CreateObject("roUrlTransfer")
    return base + "/play?magnet=" + xfer.Escape(sourceUrl)
end function

function P2PGatewayBase() as string
    sec = CreateObject("roRegistrySection", "stroku")
    if sec.Exists("p2pGateway") then return sec.Read("p2pGateway")
    return ""
end function
```

`P2PGatewayBase` lee la dirección del registro del Roku, así que **no hay IP fija en el código**.
Hace falta una pantalla de ajustes (o similar) donde el usuario escriba la dirección que muestra la app
del teléfono y se guarde con `sec.Write("p2pGateway", valor) : sec.Flush()`.

## Autodetección de la IP

No está implementada ni probada. El Roku no puede descubrir al teléfono sin un mecanismo extra (SSDP/mDNS),
que la app todavía no publica. Hasta entonces la dirección se escribe a mano en Stroku. Si el router
reasigna la IP del teléfono, conviene reservarla (DHCP estático) en el router.

## Compatibilidad de vídeo

El teléfono sirve el archivo tal cual. Si el torrent trae un códec que el Roku no reproduce
(p. ej. algunos HEVC/AC3/DTS en MKV), el Roku fallará aunque el servidor funcione.
