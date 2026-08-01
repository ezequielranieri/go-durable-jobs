# go-durable-jobs

🇬🇧 [English version](./README.md)

Durable job processor escrito en Go. Procesa trabajos asincrónicos con garantías de
at-least-once delivery, idempotencia, reintentos controlados y observabilidad básica.

> Estado: MVP completo — arquitectura, tests, benchmarks y retrospectiva documentados.
> Fase 2 (Redis, CLI de inspección, tracing distribuido) no iniciada — ver la sección de
> retrospectiva más abajo.

## Problema que resuelve

Cualquier sistema que dispare trabajo asincrónico (enviar un email, cobrar una tarjeta,
generar un reporte, notificar a un webhook externo) tarde o temprano se enfrenta a tres
preguntas incómodas:

- **¿Qué pasa si el proceso se cae a mitad de un trabajo?** Sin una cola durable, ese
  trabajo simplemente desaparece — nadie se entera, nadie lo reintenta.
- **¿Qué pasa si el mismo trabajo se dispara dos veces?** Un timeout de red hace que el
  cliente reintente un `POST`, y de repente cobraste dos veces la misma tarjeta o mandaste
  el mismo email dos veces.
- **¿Qué pasa cuando un trabajo falla?** Sin reintentos con backoff, un error transitorio
  (la base cayó un segundo, el servicio externo tuvo un blip) se convierte en una pérdida
  permanente. Sin un límite de reintentos, un error persistente satura el sistema
  reintentando para siempre.

`go-durable-jobs` es la respuesta a esas tres preguntas, resuelta con las herramientas más
simples que dan la garantía correcta — sin sumar infraestructura (colas de mensajes, brokers)
que el problema no pide todavía:

- **No perder trabajos:** Postgres como fuente de verdad. Un job encolado sobrevive a un
  crash del proceso, un reinicio, o un deploy — sigue en la tabla, `pending`, esperando a
  que un worker (el mismo u otro) lo tome.
- **No duplicar trabajos:** idempotencia real vía unique constraint. El mismo
  `idempotency_key` nunca crea dos jobs, sin importar cuántas veces llegue el request.
- **No perder trabajos por fallos transitorios, sin reintentar para siempre:** backoff
  exponencial con jitter y un límite de intentos configurable. Después de agotar los
  reintentos, el job cae a una dead letter queue explícita (no desaparece, queda disponible
  para inspección y reencolado manual).

El proyecto también sirve como ejercicio deliberado de **concurrencia correcta en Go**: el
mecanismo central (`SELECT ... FOR UPDATE SKIP LOCKED`) es la pieza que garantiza que N
workers puedan competir por la misma cola sin pisarse — verificado no solo por revisión de
código, sino con tests bajo `-race` y benchmarks reales de scaling (ver sección
[Benchmarks](#benchmarks)).

## Decisiones de diseño

Ver [`DECISIONS.md`](./DECISIONS.md) para el detalle completo de decisiones técnicas confirmadas
(idempotencia, dequeue, DLQ, circuit breaker, etc.) y su justificación.

Resumen rápido:
- **Idempotencia:** unique constraint en Postgres. Conflicto → `200 OK` con el job existente.
- **Dequeue:** `SELECT ... FOR UPDATE SKIP LOCKED` + filtro explícito de `available_at`.
- **DLQ:** misma tabla, `status = 'dead'`, con endpoint de requeue (`409` si no aplica).
- **Circuit Breaker:** no implementado en el MVP (ver retrospectiva).

## Cómo usé IA en este proyecto

Usé un asistente de IA como herramienta de pair programming durante
el desarrollo — no como autopilot. El flujo que seguí en la práctica:

- **Spec primero, código después.** Antes de escribir una línea,
  cerré el stack, el modelo de datos y las decisiones técnicas
  críticas (idempotencia, dequeue, manejo de DLQ) en una sesión de
  diseño. Eso se convirtió en la base de las "Decisiones de diseño"
  de este README.
- **Tareas chicas, revisión obligatoria antes de avanzar.** Cada
  pieza (dominio, repositorio, worker pool, HTTP, métricas) se
  planteó, se revisó su plan antes de implementar, se implementó, y
  se verificó con evidencia real (tests corriendo, `-race` limpio,
  `curl` contra el servidor en vivo) antes de pasar a la siguiente.
  Nunca aprobé una pieza solo porque "compiló".
- **La IA no reemplazó el criterio de revisión.** Sirvió para
  generar planes, implementaciones de primera pasada, y tests — pero
  encontrar los problemas reales requirió leer el código con
  atención, pedir evidencia cruda en vez de resúmenes, y cuestionar
  el "está bien" cuando algo no cerraba del todo.

Cuatro bugs concretos que este proceso evitó antes de que llegaran a
`main` (detalle completo en la
[retrospectiva](#qué-mejoraría-hoy-retrospectiva-honesta)):

1. una race condition real entre tomar un job y marcarlo en ejecución;
2. un bug que dejaba los reintentos rotos en silencio (el job nunca
   volvía a estado `pending`);
3. una asimetría en cómo se reportaban éxito y error, que iba a
   subestimar las métricas de fallos;
4. contaminación de datos entre la base de tests y la de uso manual,
   detectada porque un número no cerraba, no porque se buscara
   activamente.

Ninguno de estos bugs era obvio en una primera lectura del código
generado — todos salieron de pedir el plan antes del código, y de
verificar con evidencia real (no confiar en el resumen) antes de
aprobar cada pieza.

## Cómo levantar el proyecto

```bash
# 1. Postgres (base de uso manual, puerto host :5432)
docker-compose up -d postgres

# 2. Aplicar la migración
docker exec -i go-durable-jobs-postgres psql -U jobs -d jobs \
  < migrations/0001_create_jobs_table.up.sql

# 3. Arrancar el servidor
DATABASE_URL="postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable" \
  go run ./cmd/server
```

El servidor escucha en `:8080` (o el puerto de `APP_PORT`).

Variables de entorno (`internal/config/config.go`):

| Variable | Default | Descripción |
|---|---|---|
| `DATABASE_URL` | requerida | DSN de Postgres |
| `APP_PORT` | `8080` | Puerto HTTP |
| `NUM_WORKERS` | `5` | Cantidad de workers del pool |
| `POLL_INTERVAL` | `500ms` | Frecuencia de polling de jobs pendientes |
| `GRACE_PERIOD` | `30s` | Ventana de drenado en graceful shutdown |
| `BASE_BACKOFF_DELAY` | `1s` | Base del backoff exponencial de reintentos |

## Cómo probarlo

```bash
# 1. Encolar un job → 201
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"test.echo","payload":{"msg":"hola"},"idempotency_key":"demo-1"}'

# 2. Misma idempotency_key → 200 con el MISMO job (idempotencia, no un duplicado)
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"test.echo","payload":{"msg":"hola"},"idempotency_key":"demo-1"}'

# 3. Consultar el estado del job → GET /jobs/{id} (id de la respuesta anterior)
curl -s http://localhost:8080/jobs/<id>

# 4. Métricas en vivo
curl -s http://localhost:8080/metrics
```

En `/metrics` se ve `jobs_enqueued_total` (los duplicados idempotentes no cuentan),
`jobs_processed_total` con label `result` (`completed` o `failed`), `jobs_in_flight` y el
histograma `job_processing_duration_seconds`.

Errores de validación de negocio (en `internal/application/enqueue_job.go`) → `400`:

```bash
# type vacío → 400
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"","payload":{},"idempotency_key":"demo-bad"}'

# max_attempts negativo → 400 (0 = default 5)
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"test.echo","payload":{},"idempotency_key":"demo-bad-2","max_attempts":-1}'
```

Para correr la suite de tests de integración en una base aislada (`jobs_test`, puerto
`:5433`, servicio `postgres_test`) sin tocar la base de uso manual, ver la sección "Tests
de integración (Postgres)" — usa `TEST_DATABASE_URL` (o su default) y `scripts/setup_test_db.sh`.

### Graceful shutdown en acción

Secuencia real del servidor procesando un job cuando llega una señal de apagado: se ve cómo
el worker termina el job en curso antes de salir, sin cortarlo a la mitad.

```
2026/07/31 17:29:19 INFO go-durable-jobs starting
2026/07/31 17:29:19 INFO worker pool started num_workers=5
2026/07/31 17:29:24 INFO echo handler processing job type=test.echo payload={}
2026/07/31 17:29:24 INFO echo handler simulating slow work (5s)
2026/07/31 17:29:26 INFO signal received, starting graceful shutdown signal=interrupt
2026/07/31 17:29:29 INFO echo handler finished work
2026/07/31 17:29:29 INFO worker: job completed worker_id=2 job_id=62b6c374-abfc-47cf-a3b6-f52d1d4eed49
2026/07/31 17:29:29 INFO shutdown complete
```

La señal (SIGINT) llegó a las 17:29:26, en medio del sleep simulado de 5s que había
empezado a las 17:29:24. El proceso no corta el job: espera a que termine solo (17:29:29)
antes de loguear `shutdown complete` y cerrar. Nota: este log proviene de una prueba manual
con un sleep temporal agregado al handler para forzar esa ventana de 5s — no es el
comportamiento normal de `echoHandler`, que procesa al instante.

### Tests de integración (Postgres)

Los tests de integración corren contra una base **separada** (`jobs_test`, servicio
`postgres_test` en `docker-compose.yml`, puerto host `5433`), nunca contra la base de uso
manual (`jobs`, puerto `5432`). Esto garantiza que un test que se olvide de limpiar jamás
mezcle datos con tus pruebas manuales de `curl`.

Setup (una vez por máquina):

```bash
docker compose up -d postgres_test
scripts/setup_test_db.sh
```

Este script aplica el SQL crudo (`migrations/0001_create_jobs_table.up.sql`) con `psql -f`
solo por simplicidad local; en un flujo real de despliegue se usa el CLI de `golang-migrate`,
que trackea versiones y no requiere que el SQL sea idempotente.

Cadena de resolución de la conexión de test (`TEST_DATABASE_URL` → `DATABASE_URL` → default):

```bash
# Recomendado: base aislada para tests (jobs_test)
TEST_DATABASE_URL="postgres://jobs:jobs@localhost:5433/jobs_test?sslmode=disable" \
  go test -tags integration ./internal/infrastructure/postgres/

# Compatibilidad previa: si TEST_DATABASE_URL no está seteada, se usa DATABASE_URL.
# Si ninguna está seteada, se usa el default (jobs_test en 5433).
```

Cada test de integración limpia la tabla `jobs` al inicio de su ejecución (helper
`truncateJobs`), así ninguno depende de otro ni deja basura.

Si no levantaste el servicio `postgres_test`, los tests **se saltan** (`t.Skip`) con un
mensaje indicando qué falta levantar, en vez de fallar duro.

## Benchmarks

Benchmarks de integración contra Postgres real, con el mismo setup aislado que los tests
(`jobs_test` en el servicio `postgres_test`). Viven en
`internal/infrastructure/postgres/postgres_job_repository_bench_test.go` y usan la misma
cadena de conexión (`TEST_DATABASE_URL` → `DATABASE_URL` → default).

Medidos en: Ryzen 3 PRO 2200G (4 núcleos físicos, sin SMT), Windows, Postgres 16 en Docker.
Los números son **indicativos de este entorno**, no una cota general — repetir en hardware
distinto puede variar significativamente.

### Cómo correr

```bash
go test -tags integration -bench='Benchmark' -run='^$' -benchmem -benchtime=1000x \
  -count=5 ./internal/infrastructure/postgres/
```

Notas de ejecución:

- **`-benchtime=Nx` (iteraciones fijas), no tiempo**: cada benchmark hace `truncate` +
  reseed de datos en cada invocación; con `-benchtime=5s` el harness recalibra `b.N` en
  varias rondas y multiplica el costo de seeding sin aportar precisión. Con `Nx` el costo
  total queda acotado y predecible.
- **PowerShell**: los flags con comas hay que citarlos — `'-cpu=1,2,4,8'` — porque la coma
  se interpreta como separador de argumentos y el comando falla.

### Resultados

1000 ops por corrida. `-count=5` (Create y Dequeue), `-count=3` (ConcurrentDequeue).
Valores reportados: mediana con rango (min–max) entre corridas. `jobs_s` = operaciones por
segundo.

| Benchmark | jobs_s (mediana) | rango | p50 | p99 | allocs/op |
|---|---|---|---|---|---|
| Create | 298.5 | 210–362 | 2.7ms | 15ms | 38 |
| Dequeue (Dequeue+MarkCompleted) | 100.2 | 74–106 | 8.8ms | 31ms | 94 |
| ConcurrentDequeue (4 workers) | 231 | 231–239 | – | – | 70 |

La **varianza entre corridas** viene de la cola, no de la mediana: el `p50` es estable en
cada run (Create 2.5–3.0ms, Dequeue 8.7–11.0ms), pero unos pocos ops con latencia de
20–50ms desplazan la media. La causa más probable —hipótesis razonada, **no verificada**:
no se corrió `iostat` ni `GODEBUG=gctrace=1` para confirmarla— es el `fsync` del WAL en cada
`COMMIT` (cada operación es un commit) bajo Docker-on-Windows, con latencias de fsync muy
variables.
Por eso `ns/op` (que es la media) oscila entre corridas mientras `p50` no; este README
reporta mediana y p50 como métricas estables.

### Scaling de `SKIP LOCKED` bajo concurrencia

`BenchmarkConcurrentDequeue`, 1000 ops, `-count=3`, variando el número de workers con `-cpu`:

```bash
go test -tags integration -bench='BenchmarkConcurrentDequeue' -run='^$' -benchmem \
  -benchtime=1000x '-cpu=1,2,4,8' -count=3 ./internal/infrastructure/postgres/
```

| `-cpu` (workers) | jobs_s (mediana) |
|---|---|
| 1 | 143 |
| 2 | 191 (+33%) |
| 4 | 231 (+21%) |
| 8 | 229 (≈0%) |

Conclusión: **no hay señal de degradación por contención de locks**. El throughput crece
hasta ~4 workers y se aplana — el techo lo pone el hardware (4 núcleos físicos), no el
mecanismo de dequeue. `-cpu=8` sobresuscribe los 4 núcleos reales y no gana nada. Es el
comportamiento esperado de un dequeue bien diseñado: el punto de saturación es el Postgres
(si la hipótesis del `COMMIT` con fsync es correcta), no la contención de `SKIP LOCKED`.

### Nota metodológica

Antes de confiar en los números verificamos el propio harness de benchmarking y
encontramos dos problemas:

1. **`b.ResetTimer()` no reinicia el timer detenido por `StopTimer()`** — solo lo detiene y
   pone el acumulado en cero; hay que llamar `b.StartTimer()` después. Nuestra primera
   corrida reportó `+Inf jobs_s` y `ns/op` ausente justamente por esto. El patrón correcto
   es `StopTimer → setup → StartTimer → bucle → StopTimer`.
2. **Escaping de comas en PowerShell** — `-cpu=1,2,4,8` sin comillas se parsea como array
   de argumentos y el comando falla. Debe pasarse citado: `'-cpu=1,2,4,8'`.

El primer resultado que salió (un run único, con el timer mal usado) **no** es el que
aparece en este README. Los números de arriba vienen de `-count=3`/`-count=5` con el patrón
corregido.

## Métricas

Expuestas en `GET /metrics` en formato Prometheus (`internal/telemetry/metrics.go`):

| Métrica | Tipo | Qué mide |
|---|---|---|
| `jobs_enqueued_total` | Counter | Jobs nuevos encolados (los duplicados idempotentes no cuentan) |
| `jobs_processed_total{result}` | Counter | Intentos de procesamiento, por resultado (`completed`/`failed`) — `failed` incluye tanto fallos de negocio del handler como fallos de persistencia posterior (`MarkCompleted`/`MarkFailed`) |
| `jobs_in_flight` | Gauge | Jobs siendo procesados en este momento |
| `job_processing_duration_seconds` | Histogram | Duración de cada intento (handler + `Mark*`), buckets custom hasta 300s |

Comparar `jobs_enqueued_total` contra la suma de `jobs_processed_total` es la forma más
simple de detectar jobs "perdidos" (encolados pero nunca procesados) en un vistazo.

## Qué mejoraría hoy (retrospectiva honesta)

Este es un proyecto de portfolio que resuelve un problema real con una arquitectura sólida,
pero no es un sistema de producción. Esta sección deja explícito qué se decidió no hacer y
por qué, qué haría distinto con más tiempo y qué funcionó mejor de lo esperado — el mismo
criterio de honestidad que la sección de benchmarks.

### Decisiones conscientes de no implementar

**Circuit Breaker.** Los jobs del MVP son genéricos y no llaman servicios externos por
defecto: no hay nada que proteger de un fallo descendente, y un circuit breaker sin
dependencias externas solo agrega complejidad. Si en el futuro se agregara un tipo de job
que sí invoque APIs externas, el circuit breaker iría en el handler de negocio de ese job
(por host/API, con umbral de fallos y cooldown antes de abrir), nunca en la maquinaria de
dequeue/encolado, que no es la capa que falla en ese escenario. (Detalle completo en
`DECISIONS.md`, sección 2.4.)

**Redis.** Quedó en Fase 2 porque Postgres con la unique constraint de `idempotency_key`
ya cubre la idempotencia del MVP de forma atómica y durable, sin una pieza extra que
operar. Redis sumaría un chequeo de idempotencia ultra-rápido (una lectura en vez de un
round trip completo) y rate limiting — valioso para un productor de alto volumen, no
crítico a la escala actual.

### Qué haría distinto con más tiempo

- **La varianza de los benchmarks no está resuelta del todo.** La hipótesis del `fsync`
  bajo Docker-on-Windows quedó documentada, no confirmada. Con más tiempo correría la
  suite en Linux nativo o con `iostat` / `GODEBUG=gctrace=1` para descartar o confirmar
  la causa real.
- **`echoHandler` es un placeholder.** El pool recibe un único handler hardcodeado en
  `main.go` que loguea y devuelve `nil`. Un sistema real necesitaría un registro que
  mapee `type` → handler de negocio; la interfaz ya está lista
  (`Handle(ctx, jobType, payload)`), falta la pieza de dispatch.
- **La API HTTP no tiene autenticación ni autorización.** Cualquiera que llegue al puerto
  puede encolar y reencolar jobs. Aceptable para un portfolio local; inviable tal cual en
  producción.
- **Tracing distribuido (OpenTelemetry) quedó en Fase 2.** Tiene sentido si el sistema
  crece a varios servicios; para un proceso único es sobre-ingeniería.
- **El CLI de inspección/reencolado nunca se implementó.** Estaba en el plan original y
  quedó fuera: hoy el único reencolado es `POST /jobs/{id}/requeue` y no hay forma de
  listar todos los jobs `dead` de una pasada. En la práctica, sería la primera adición de
  Fase 2.

### Qué funcionó bien (y por qué)

El proceso de revisión capa por capa, con corrección real antes de avanzar, fue lo que más
bugs evitó — al menos cuatro habrían sido difíciles de debuggear a posteriori:

- el race condition original entre Dequeue y MarkRunning (dos workers podían tomar el
  mismo job);
- el bug en MarkFailed dejaba los jobs con reintentos pendientes en un estado (`failed`)
  que Dequeue nunca vuelve a mirar — los reintentos quedaban rotos en la práctica aunque
  el backoff estuviera bien calculado. Es el tipo de bug más peligroso: no rompe nada de
  inmediato, se manifiesta como "los reintentos simplemente no pasan" mucho después;
- la asimetría de retorno en ProcessJob (éxito y error no devolvían de forma consistente);
- la contaminación cruzada entre la base de integración y la de uso manual: los jobs
  leftover de los tests aparecían procesados en las métricas en vivo. El sistema
  funcionaba, pero los números decían algo que no era lo que parecía; se detectó
  revisando en vez de asumir, y derivó en el aislamiento físico de las bases (`jobs_test`
  en el puerto 5433).

Encontrarlos en revisión, cuando cada capa era chica y el contexto estaba fresco, costó
minutos; encontrarlos después, con workers corriendo, habría costado horas y un debugging
mucho más doloroso.
