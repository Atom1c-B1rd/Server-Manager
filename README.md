# Go Server Manager

Aplicación local para crear, configurar y administrar servidores desde una interfaz web. Permite definir servidores de distintos protocolos y personalizar su comportamiento mediante rutas y handlers.

## Características

- **HTTP:** creación y configuración de servidores HTTP.
- **TCP:** recepción y despacho de mensajes por tipo.
- **UDP:** recepción y despacho de datagramas por tipo.
- **WebSocket:** rutas de conexión independientes y handlers asociados a cada ruta.
- **Handlers personalizados:** lógica escrita en Go y ejecutada mediante [Yaegi](https://github.com/traefik/yaegi).
- **Gestión desde el navegador:** creación, edición, inicio y detención de servidores.
- **Logs:** registros de actividad y mensajes para facilitar el diagnóstico.
- **Configuración persistente:** datos de los servidores y logs guardados localmente.
- **Documentación integrada:** guía de uso y descripción del funcionamiento accesible desde la propia interfaz.

## Requisitos

- **Go 1.24.3 o superior compatible con el módulo**.
- Acceso a Internet la primera vez que se descargan las dependencias de Go.

Las dependencias del proyecto están declaradas en `go.mod` y `go.sum`. No es necesario instalarlas una por una: Go las descarga al ejecutar los comandos correspondientes.

> Los handlers pueden ejecutar código personalizado. Ejecutá únicamente código que entiendas y en el que confíes.

## Instalación y ejecución

1. Instalá Go desde [go.dev/dl](https://go.dev/dl/).
2. Cloná el repositorio:

   ```bash
   git clone https://github.com/Atom1c-B1rd/Server-Manager.git
   cd Server-Manager
   ```

3. Descargá las dependencias:

   ```bash
   go mod download
   ```

4. Iniciá la aplicación desde la carpeta que contiene `go.mod`:

   ```bash
   go run .
   ```

5. Abrí la interfaz en el navegador:

   **http://localhost:3000**

La interfaz está configurada para escuchar en `localhost`, por lo que está pensada para el uso local en la máquina donde se ejecuta el programa.

## Uso básico

1. Abrí la interfaz web.
2. Creá un servidor y elegí el protocolo.
3. Configurá el puerto y las opciones disponibles.
4. Agregá rutas cuando corresponda y definí los handlers para procesar las solicitudes o mensajes.
5. Guardá los cambios e iniciá el servidor.
6. Consultá los logs para revisar conexiones, mensajes y errores.

La sección de documentación de la interfaz describe el funcionamiento de cada protocolo y el formato esperado por sus handlers.

## Formatos de mensajes

Los protocolos no comparten necesariamente el mismo formato de entrada. En términos generales:

- **TCP:** mensajes JSON delimitados por salto de línea.
- **UDP:** datagrama con un tipo de mensaje seguido de `\n` y, opcionalmente, datos binarios.
- **WebSocket:** mensajes que pueden incluir un campo de tipo y un campo `data`; los handlers se pueden asociar a una ruta.
- **HTTP:** solicitudes procesadas según la configuración del servidor y sus handlers.

Consultá `PROTOCOL_FORMATS.md` para ver los detalles de los formatos de protocolo.

## Estructura del proyecto

```text
.
├── Data/       # Modelos y almacenamiento de configuración
├── Managers/   # Gestión del ciclo de vida y configuración de servidores
├── Runtime/    # Ejecución de handlers y gestión de dependencias
├── Servers/
│   ├── HTTP/
│   ├── TCP/
│   ├── UDP/
│   └── WebSocket/
├── UI/         # Interfaz web y documentación integrada
├── tests/      # Pruebas
├── go.mod
├── go.sum
└── main.go
```

## Datos locales

El proyecto guarda la configuración de los servidores y sus logs en el directorio `Data/`. Conservá ese directorio si querés mantener esos datos entre ejecuciones. Revisá su contenido antes de compartirlo públicamente, ya que puede incluir configuraciones creadas durante el uso.

## Dependencias principales

Las dependencias directas están especificadas en `go.mod`. Entre ellas se encuentran:

- [Echo](https://github.com/labstack/echo) — componentes HTTP.
- [Gorilla WebSocket](https://github.com/gorilla/websocket) — comunicación WebSocket.
- [Yaegi](https://github.com/traefik/yaegi) — intérprete de Go para handlers.
- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) — driver de MySQL.
- [lib/pq](https://github.com/lib/pq) — driver de PostgreSQL.

No hace falta configurar una base de datos para iniciar la interfaz; las conexiones a bases de datos dependen de la configuración que se use.

## Estado del proyecto

Proyecto en desarrollo. La estructura y las funcionalidades pueden cambiar entre versiones.

## Licencia

Todavía no se especificó una licencia. Agregá un archivo `LICENSE` antes de distribuir el proyecto con condiciones de uso definidas.
