package br.com.freela.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import java.util.UUID;

@Component
public class NotificacaoConsumer {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoConsumer.class);
    private final NotificacaoService service;
    private final JsonMapper mapper;

    public NotificacaoConsumer(NotificacaoService service, JsonMapper mapper) {
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

        log.info("notificacao.evento.recebido eventId={} contratoId={} eventType={} correlationId={}",
                eventId, contratoId, tipo, correlationId);

        JsonNode payload = evento.path("payload");
        UUID clienteId = UUID.fromString(payload.path("clienteId").asString());
        UUID freelancerId = UUID.fromString(payload.path("freelancerId").asString());
        String titulo = payload.path("titulo").asString();

        switch (tipo) {
            case "ContratoCriado" ->
                    service.registrar(eventId, contratoId, freelancerId, tipo, "Novo contrato: " + titulo);
            case "EntregaRegistrada" ->
                    service.registrar(eventId, contratoId, clienteId, tipo, "Entrega registrada no contrato: " + titulo);
            case "ContratoConcluido" ->
                    service.registrar(eventId, contratoId, freelancerId, tipo, "Contrato concluído: " + titulo);
            case "ContratoCancelado" ->
                    service.registrar(eventId, contratoId, freelancerId, tipo, "Contrato cancelado: " + titulo);
            default ->
                    log.warn("notificacao.evento.ignorado eventId={} eventType={}", eventId, tipo);
        }
    }
}