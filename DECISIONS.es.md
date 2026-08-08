# DECISIONS.es.md — Constitución del proyecto `go-durable-jobs`

🇬🇧 [English version](./DECISIONS.md)

Este archivo es la fuente de verdad para cualquier agente (humano o IA) que trabaje en este
repositorio. Toda implementación debe respetar estas reglas. Si una tarea entra en conflicto
con algo definido aquí, **detenerse y consultar antes de proceder**.

Cada decisión registra: la regla (lo que el código DEBE hacer), las alternativas rechazadas
y por qué, y el costo que el proyecto acepta por elegirla. Esta última parte importa: saber
cuánto cuesta una decisión es lo que permite reabrirla más adelante con una justificación
sólida en lugar de una corazonada.

Leyenda de estado: `Accepted` = asentado; `Open` = propuesta, no implementar.

---

## 1. Stack tecnológico (fijo, no negociable sin discusión explícita)

- **Lenguaje:** Go 1.22+
- **Persistencia:** PostgreSQL (source of truth, único mecanismo de idempotencia en el MVP)
- **Cache / Redis:** NO se usa en el MVP. Queda reservado para Fase 2 (idempotencia rápida,
  rate limiting).
- **Concurrencia:** Worker pool nativo con `channels`, `context` y `errgroup`. Prohibido usar
  librerías de colas externas (Kafka, RabbitMQ, etc.) en el MVP.
- **Arquitectura:** Clean Architecture / Hexagonal. El dominio no depende de infraestructura.
- **HTTP:** `chi` o `net/http` estándar.
- **Migraciones:** `golang-migrate`, versionadas y reversibles.
- **Observabilidad:** `slog` para logs estructurados + Prometheus para métricas.
- **Tracing:** OpenTelemetry — Fase 2, no implementar todavía.
- **Testing:** paquete `testing` + `testcontainers` + race detector (`go test -race`).
- **Infra local:** Docker Compose (Postgres + app únicamente en el MVP).

**Por qué estas elecciones (y qué se rechazó)**
- **Redis** se difiere deliberadamente a la Fase 2 (ver §4): la unique constraint de
  `idempotency_key` ya brinda deduplicación atómica y durable sin nada extra que operar.
  Redis agregaría una verificación ultrarrápida en el camino de lectura y rate limiting —
  valioso para un productor de alto volumen, no a escala MVP.
- **Librerías de colas externas (Kafka, RabbitMQ, etc.)** prohibidas en el MVP: una tabla
  de Postgres + `SKIP LOCKED` es suficiente para esta carga de trabajo y mantiene la fuente
  de verdad en un solo lugar. Un broker es un costo operativo que el problema aún no exige.
- **`chi` vs `net/http` estándar**: ambos son aceptables. Este "o" queda deliberadamente
  abierto — elegir uno durante la implementación y documentarlo aquí.
- **`testcontainers` sobre un harness propio**: brinda semántica real de Postgres (locking,
  `SKIP LOCKED`) en CI con limpieza por test, en lugar de un mock que probaría el código
  pero no la garantía.
- **OpenTelemetry** es una postergación explícita a la Fase 2 (§4): para un único proceso,
  el tracing distribuido es sobre-ingeniería; justifica su costo solo cuando el sistema se
  divide en varios servicios.
- **`slog` + Prometheus sobre un framework de logging**: primero la stdlib. `slog` cubre
  los logs estructurados; Prometheus cubre las métricas; un framework de logging de
  terceros agregaría una dependencia sin ningún vacío que llenar.

---

## 2. Decisiones de diseño confirmadas (no reabrir sin justificación fuerte)

> Estado de cada subsección: `Accepted` — no reabrir sin justificación sólida (ver preámbulo).

### 2.1 Idempotencia — Estado: Accepted

**Regla**
- Unique constraint en `idempotency_key` a nivel de base de datos.
- Si `POST /jobs` recibe una key que ya existe: responder **`200 OK`** con el job existente
  en el body (NUNCA un 500). Este comportamiento imita a Stripe: el cliente que reintenta
  espera éxito, no un error a manejar distinto.

**Alternativas rechazadas**
- *Check-then-insert a nivel de aplicación* (SELECT, y luego INSERT): tiene condición de carrera. Dos requests concurrentes pasan ambas el chequeo y ambas insertan → jobs duplicados. Solo la constraint de la base de datos es atómica.
- *Redis SETNX como fuente de deduplicación*: rápido, pero agrega infraestructura y deja de ser veraz tras un crash — Postgres es la fuente de verdad, Redis no. Queda para la Fase 2 como *optimización* del camino caliente, nunca como la garantía.
- *Idempotencia por clave natural* (`type` + hash del `payload`): dos jobs legítimamente idénticos se vuelven indistinguibles. La clave opaca del cliente es el contrato más limpio.
- *201 en la primera creación, 200 en replay*: obliga al cliente a manejar dos códigos de éxito; la única respuesta de éxito de Stripe es más simple de consumir.

**Costo aceptado**
- Cada `INSERT` compite por el índice único; con un throughput de enqueue muy alto ese índice se vuelve un hotspot (irrelevante a escala MVP, revisar en Fase 2).
- `200 OK` en el replay no le dice al cliente si el job sigue pendiente o ya terminó — `GET /jobs/{id}` es la sonda de estado, no la respuesta del POST.
- El cliente es dueño del ciclo de vida de la clave: si la pierde, no puede deduplicar.

### 2.2 Dequeue de jobs — Estado: Accepted

**Regla**
- Usar **`SELECT ... FOR UPDATE SKIP LOCKED`** para que los workers tomen jobs sin race
  conditions.
- La query DEBE filtrar explícitamente `available_at <= NOW()` en el `WHERE` (no confiar
  solo en el índice parcial) para que el delay funcione correctamente.
- El orden del dequeue es **`priority DESC, created_at ASC`**: `priority` es un campo de
  contrato de primera clase en `POST /jobs` (default `0`, gana el mayor) y `delay_seconds`
  del request se guarda como `available_at = NOW() + delay`, así que un job con delay
  simplemente no es visible hasta su momento. El índice parcial `idx_jobs_status_available`
  cubre este orden sin un sort extra.

**Alternativas rechazadas**
- *`LISTEN`/`NOTIFY` de Postgres*: impulsado por eventos en vez de polling, pero las notificaciones no tienen garantía de entrega — un worker que se suscribe tarde se pierde eventos en silencio, y de todos modos se necesita un fallback de polling. El polling de 500ms es más simple y sin pérdidas.
- *Advisory locks (`pg_advisory_lock`)*: viable, pero se implementa a mano la semántica de cola (orden, delays, dead-letter) que `SKIP LOCKED` brinda de forma nativa.
- *Claim mediante `UPDATE ... WHERE id IN (SELECT ...)` sin row lock*: dos workers pueden leer la misma fila y ambos reclamarla — la carrera exacta que este diseño existe para prevenir.
- *Un solo worker serial*: cero contención, pero cero paralelismo; contradice el propósito.

**Costo aceptado**
- El polling agrega latencia: un job ejecutable puede esperar hasta `POLL_INTERVAL` (500ms) antes de ser tomado. Aceptable para trabajo asíncrono, irrelevante para jobs batch.
- Bajo contención intensa, `SKIP LOCKED` salta filas bloqueadas en silencio — la cola sigue correcta, pero la latencia de cola puede estirarse; la ventana de lock es de una fila y corta, así que en la práctica solo se ve en colas saturadas.
- Cada dequeue es una *escritura* (row lock + cambio de estado), así que el camino de dequeue no escala como lo haría una lectura pura — inherente a cualquier cola basada en claims.

### 2.3 Dead Letter Queue (DLQ) — Estado: Accepted

**Regla**
- NO es una tabla ni cola separada. Es la misma tabla `jobs` con `status = 'dead'`.
- Debe existir una forma de reencolar: endpoint `POST /jobs/{id}/requeue`.
  - Solo permite reencolar si `status = 'dead'` (guarda explícita en el `WHERE` del UPDATE).
  - Si el job no está en `dead`: responder **`409 Conflict`**.
  - Al reencolar: `status = 'pending'`, `attempts = 0`, `last_error = NULL`,
    `updated_at = NOW()`, `available_at = NOW()`.

**Alternativas rechazadas**
- *Tabla o cola dead-letter separada*: agrega un segundo camino de persistencia y un paso de transferencia; el historial del job (`attempts`, `last_error`) quedaría repartido entre tablas y sería más difícil de inspeccionar. La misma tabla `jobs` con `status = 'dead'` mantiene una única fuente de verdad y una sola migración.
- *`DELETE` físico ante el fallo final*: destruye el registro — el valor explícito del proyecto es conservar el histórico (soft states: `dead`, `completed`) y mantener el job disponible para inspección y requeue manual. El DELETE vuelve la DLQ invisible e irrecuperable.
- *Cola dead-letter externa (DLQ del lado del broker)*: infraestructura prohibida en el MVP — la misma razón que para la cola principal.

**Costo aceptado**
- Los jobs `dead` permanecen en la tabla indefinidamente hasta ser reencolados — no hay TTL ni archivo, así que la tabla crece con el histórico (acotado solo por la higiene operativa).
- Toda query que quiera solo jobs "vivos" debe filtrar por status; el status es una señal soft, así que un `WHERE status = ...` olvidado mezcla jobs `dead` de vuelta en la cola en silencio.
- El requeue es una operación manual y explícita (un endpoint `POST`), nunca automática — un job `dead` permanece `dead` a menos que alguien actúe.

### 2.4 Circuit Breaker — Estado: Accepted (deliberadamente NO implementado en el MVP)

**Regla**
- NO implementar en el MVP. Los jobs son genéricos y no llaman servicios externos por
  defecto. Si en el futuro se agrega un tipo de job que sí llama APIs externas, documentar
  ahí dónde y por qué se agregaría un circuit breaker.
- Esta decisión debe quedar explícita en la retrospectiva del README, no omitida en silencio.

**Alternativas rechazadas**
- *Implementarlo ahora para un caso genérico*: los jobs del MVP no llaman servicios externos, así que un breaker no protegería nada — complejidad pura sin ninguna falla que pueda abrir.
- *Implementarlo en la maquinaria de dequeue/enqueue*: capa equivocada — la retrospectiva del README es explícita: si en el futuro un tipo de job llama APIs externas, el breaker vive en el business handler de ese job (por host/API, con umbral de fallos y cooldown antes de abrir), nunca en la maquinaria de la cola, que no es la capa que falla en ese escenario.

**Costo aceptado**
- No existe protección para un futuro tipo de job que llame servicios externos; agregarla después es un paso deliberado y documentado, no algo retrofitable sin tocar el handler del job.
- Si llega un job con dependencia externa y alguien olvida la nota del §2.4, no hay nada en el framework que lo sugiera — la brecha queda documentada, no impuesta.

### 2.5 Retries — Estado: Accepted

**Regla**
- Exponential backoff + jitter.
- `max_attempts` configurable por job (default 5). Al superarlo, pasa a `status = 'dead'`.
- Un **panic en el handler del job se trata como un fallo reintentable**: `ProcessJob` lo
  recupera, registra el panic en `last_error` y marca el job como fallido con el backoff
  normal (o `dead` cuando se agotan los intentos). Un handler que paniquea nunca debe
  matar al worker ni tumbar el pool.

**Alternativas rechazadas**
- *Número fijo de reintentos sin backoff*: reintentar de inmediato vuelve a golpear la dependencia o el DB que falla — la premisa del README (los parpadeos transitorios se vuelven pérdida permanente, los errores persistentes saturan el sistema) aboga por espaciar los intentos.
- *Reintentar para siempre sin límite*: un error persistente reintenta sin fin y satura el sistema; el límite explícito (`max_attempts`, default 5) es lo que mueve el job a `dead`, donde queda visible.
- *Reintento inmediato sin jitter*: sin jitter, N workers reintentando el mismo job fallido se sincronizan en picos de thundering herd; backoff + jitter los distribuye.

**Costo aceptado**
- La latencia de reintento se acumula: un job con `max_attempts` 5 puede tardar minutos en total (base 1s+2s+4s+8s) antes de caer a `dead` — la detección de un fallo persistente no es instantánea.
- `max_attempts` es un default por job, no una imposición global; cada llamador debe fijarlo conscientemente (o apoyarse en el default), así que un llamador puede elegir configuraciones patológicas.
- Durante la ventana de backoff el job queda en `pending` pero no es ejecutable hasta `available_at`; quien filtra solo por status ve una imagen engañosa — el conjunto realmente ejecutable es `pending AND available_at <= NOW()`.

### 2.6 Graceful shutdown — Estado: Accepted

**Regla**
- `context` + `signal.Notify`. El servidor debe esperar a que los workers activos terminen
  su job actual antes de salir. Nunca cortar un job a la mitad.
- Un job en vuelo corre con **`context.Background()`**, deliberadamente NO con el context
  cancelado del worker: `Pool.Shutdown` cancela el context de polling para detener nuevos
  dequeues, pero el job que se está procesando conserva su propio context no cancelable y
  corre hasta terminar. Esto es lo que hace que "nunca cortar un job a la mitad" se cumpla
  a nivel de goroutine incluso después de recibir una señal.

**Alternativas rechazadas**
- *Matar con `context.Cancel` sin drain*: cancelar a mitad del job aborta el procesamiento y puede dejar el job en un estado incorrecto (`running` para siempre, o a medias) — el log de shutdown del README muestra la secuencia deseada: terminar el job en vuelo y luego salir.
- *Salida forzada con jobs activos*: matar el proceso con jobs en vuelo arriesga el camino de confirmación; con Postgres como fuente de verdad el job nunca se pierde, pero su estado queda `running` y estancado hasta una intervención manual — nada lo reaprovecha.
- *Drain corto fijo y luego SIGKILL*: un timeout rígido puede igualmente cortar un job largo a la mitad — exactamente lo que la regla prohíbe ("nunca cortar un job a la mitad").

**Costo aceptado**
- El tiempo de shutdown está acotado por el job en vuelo más lento: si un job tarda minutos, el proceso sigue vivo ese tiempo (la ventana de drain `GRACE_PERIOD` existe para acotar esto).
- Durante el drain no se toman jobs nuevos, así que un shutdown puede frenar brevemente la cola — el throughput baja mientras el pool se apaga.
- `signal.Notify` + espera es un patrón de coordinación: solo funciona si cada worker realmente chequea el context, así que la garantía vale lo que valga la disciplina del código.

### 2.7 Aislamiento del DB de tests de integración — Estado: Accepted

**Regla**
- Los tests de integración corren contra un DB **físicamente separado** (`jobs_test`,
  servicio `postgres_test` en docker-compose, puerto host `5433`, volumen propio),
  nunca contra la base de uso manual (`jobs`, puerto `5432`).
- El truncate por test (`truncateJobs`) es higiene defensiva, no la garantía: aunque un
  test futuro se olvide de limpiar, es imposible que toque datos manuales.
- Cadena de conexión de test: `TEST_DATABASE_URL` → `DATABASE_URL` → default (`jobs_test`).
- La migración `0001` lleva `IF NOT EXISTS` como caso puntual para que
  `scripts/setup_test_db.sh` (que aplica SQL crudo con `psql -f`) sea re-ejecutable.
  Esto NO establece norma: el flujo real usa golang-migrate, que trackea versiones y no
  requiere idempotencia en el SQL.

**Alternativas rechazadas**
- *Base de uso manual compartida*: un test que olvida limpiar contamina los datos y las métricas en vivo — la retrospectiva del README documenta exactamente este bug (jobs sobrantes de tests aparecieron como procesados en las métricas). La separación física es la única solución hermética.
- *Mock sqlite en proceso para la integración*: sqlite no implementa la semántica de locking de Postgres (`SKIP LOCKED`, `FOR UPDATE`) que los tests de integración necesitan; un mock probaría el código, no la garantía.
- *Esquema lógico separado en la misma DB*: dos esquemas en un mismo Postgres comparten el servidor y su fsync/vacuum/disco; un DSN accidental o un rol compartido puede alcanzar igual los datos manuales. Un segundo servicio en puerto/volumen propios (`5433`) elimina el modo de fallo por completo.

**Costo aceptado**
- Un segundo servicio de Postgres que ejecutar y mantener localmente (contenedor extra, volumen propio, puerto propio) — el setup requiere el servicio `postgres_test` + `scripts/setup_test_db.sh`.
- Los tests necesitan una cadena de conexión separada (`TEST_DATABASE_URL` → `DATABASE_URL` → default); el fallback a `DATABASE_URL` es compatibilidad hacia atrás deliberada y, si se configura mal, puede apuntar los tests a la base manual — la garantía se cumple solo si se respeta la cadena.
- El truncate por test agrega tiempo de setup a cada corrida (cada test resiembra sus datos).

---

## 3. Convenciones de código

- Errores de dominio y errores de infraestructura se modelan por separado (tipos distintos,
  nunca mezclar un error de Postgres crudo en la capa de dominio).
- Todo acceso a Postgres pasa por `internal/infrastructure/postgres`, nunca se llama SQL
  directo desde `handler` o `application`.
- Los use cases viven en `internal/application` y solo dependen de interfaces (puertos)
  definidas en `internal/domain`, nunca de implementaciones concretas.
- Nombres de tablas y columnas en `snake_case`. Nombres de tipos y funciones Go en el
  estilo idiomático (`CamelCase` / `camelCase`).

## 4. Prohibiciones explícitas

- No agregar Redis, Kafka, RabbitMQ, Temporal ni ninguna infra adicional sin que quede
  documentada la razón y aprobada explícitamente.
- No implementar Circuit Breaker ni OpenTelemetry en el MVP.
- No devolver `500 Internal Server Error` ante un conflicto de idempotencia esperado.
- No hacer `DELETE` físico de jobs. El histórico se conserva (soft states: `dead`, `completed`).

## 5. Roadmap de referencia (alto nivel)

1. Estructura + dominio + Postgres + encolar job + tests básicos
2. Worker pool + `SKIP LOCKED` + estados + graceful shutdown
3. Retries + DLQ + requeue
4. Métricas Prometheus + logs estructurados + API completa
5. Tests de carrera, testcontainers, benchmarks
6. Documentación y retrospectiva final en el README
