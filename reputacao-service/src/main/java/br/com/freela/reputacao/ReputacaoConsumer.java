package br.com.freela.reputacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import java.util.UUID;

@Component
public class ReputacaoConsumer {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoConsumer.class);
    private final ReputacaoService service;
    private final JsonMapper mapper;

    public ReputacaoConsumer(ReputacaoService service, JsonMapper mapper) {
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

        MDC.put("correlationId", correlationId);
        try {
            log.info("reputacao.evento.recebido eventId={} contratoId={} eventType={}", eventId, contratoId, tipo);

            if (!"ContratoConcluido".equals(tipo)) {
                log.info("reputacao.evento.ignorado eventId={} eventType={}", eventId, tipo);
                return;
            }

            JsonNode payload = evento.path("payload");
            UUID freelancerId = UUID.fromString(payload.path("freelancerId").asString());
            service.registrarContratoConcluido(eventId, contratoId, freelancerId, payload.path("valor").decimalValue());
        } finally {
            MDC.remove("correlationId");
        }
    }
}