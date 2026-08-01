# DECISIONS.md — Constitución del proyecto `go-durable-jobs`

Este archivo es la fuente de verdad para cualquier agente (humano o IA) que trabaje en este
repositorio. Toda implementación debe respetar estas reglas. Si una tarea entra en conflicto
con algo definido acá, se detiene y se consulta antes de proceder.

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

---

## 2. Decisiones de diseño confirmadas (no reabrir sin justificación fuerte)

### 2.1 Idempotencia
- Unique constraint en `idempotency_key` a nivel de base de datos.
- Si `POST /jobs` recibe una key que ya existe: responder **`200 OK`** con el job existente
  en el body (NUNCA un 500). Este comportamiento imita a Stripe: el cliente que reintenta
  espera éxito, no un error a manejar distinto.

### 2.2 Dequeue de jobs
- Usar **`SELECT ... FOR UPDATE SKIP LOCKED`** para que los workers tomen jobs sin race
  conditions.
- La query DEBE filtrar explícitamente `available_at <= NOW()` en el `WHERE` (no confiar
  solo en el índice parcial) para que el delay funcione correctamente.

### 2.3 Dead Letter Queue (DLQ)
- NO es una tabla ni cola separada. Es la misma tabla `jobs` con `status = 'dead'`.
- Debe existir una forma de reencolar: endpoint `POST /jobs/{id}/requeue`.
  - Solo permite reencolar si `status = 'dead'` (guarda explícita en el `WHERE` del UPDATE).
  - Si el job no está en `dead`: responder **`409 Conflict`**.
  - Al reencolar: `status = 'pending'`, `attempts = 0`, `last_error = NULL`,
    `updated_at = NOW()`, `available_at = NOW()`.

### 2.4 Circuit Breaker
- NO implementar en el MVP. Los jobs son genéricos y no llaman servicios externos por
  defecto. Si en el futuro se agrega un tipo de job que sí llama APIs externas, documentar
  ahí dónde y por qué se agregaría un circuit breaker.
- Esta decisión debe quedar explícita en la retrospectiva del README, no omitida en silencio.

### 2.5 Retries
- Exponential backoff + jitter.
- `max_attempts` configurable por job (default 5). Al superarlo, pasa a `status = 'dead'`.

### 2.6 Graceful shutdown
- `context` + `signal.Notify`. El servidor debe esperar a que los workers activos terminen
  su job actual antes de salir. Nunca cortar un job a la mitad.

### 2.7 Aislamiento del DB de tests de integración
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
