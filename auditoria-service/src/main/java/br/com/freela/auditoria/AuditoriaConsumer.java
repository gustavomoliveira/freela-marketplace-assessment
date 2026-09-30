package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import java.time.Instant;
import java.util.UUID;

@Component
public class AuditoriaConsumer {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaConsumer.class);
    private final AuditoriaService service;
    private final JsonMapper mapper;

    public AuditoriaConsumer(AuditoriaService service, JsonMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "contrato.eventos")
    public void consumir(String mensagem) {
        JsonNode evento = mapper.readTree(mensagem);
        UUID eventId = UUID.fromString(evento.path("eventId").asString());
        UUID contratoId = UUID.fromString(evento.path("contratoId").asString());
        String tipo = evento.path("eventType").asString();
        String correlationId = evento.path("correlationId").asString();
        Instant occurredAt = Instant.parse(evento.path("occurredAt").asString());

        MDC.put("correlationId", correlationId);
        try {
            log.info("auditoria.evento.recebido eventId={} contratoId={} eventType={}", eventId, contratoId, tipo);
            service.registrar(eventId, contratoId, tipo, correlationId, occurredAt, mensagem);
        } finally {
            MDC.remove("correlationId");
        }
    }
}