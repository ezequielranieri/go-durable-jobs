package handler

// El endpoint GET /metrics lo registra el ServeMux apuntando al
// metricsHandler inyectado en New (el promhttp.Handler construido por
// telemetry.NewMetrics). El paquete handler no importa el cliente de
// Prometheus: recibe un http.Handler genérico.
