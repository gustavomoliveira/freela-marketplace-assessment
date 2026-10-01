package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.infrastructure.persistence.SpringDataOutboxRepository;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublicador {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublicador.class);
    private final SpringDataOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Tracer tracer;
    private final Propagator propagator;
    private final JsonMapper mapper;

    public OutboxPublicador(SpringDataOutboxRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                            Tracer tracer, Propagator propagator, JsonMapper mapper) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer;
        this.propagator = propagator;
        this.mapper = mapper;
    }

    @Scheduled(fixedDelayString = "${freela.outbox.intervalo-ms:1000}")
    @Transactional
    public void publicarPendentes() {
        var pendentes = repository.findTop50ByPublicadoEmIsNullOrderByIdAsc();
        if (pendentes.isEmpty()) return;
        log.info("contrato.outbox.publicacao.lote quantidade={}", pendentes.size());

        for (var mensagem : pendentes) {
            MDC.put("correlationId", mensagem.getCorrelationId());
            Span span = propagator.extract(lerTraceContext(mensagem.getTraceContext()), Map::get)
                    .name("outbox.publicar")
                    .tag("contratoId", mensagem.getContratoId().toString())
                    .start();
            try (Tracer.SpanInScope ws = tracer.withSpan(span)) {
                log.info("contrato.outbox.publicacao.inicio contratoId={} eventId={} eventType={} topico={}",
                        mensagem.getContratoId(), mensagem.getEventId(), mensagem.getEventType(), mensagem.getTopico());
                var resultado = kafkaTemplate.send(mensagem.getTopico(), mensagem.getContratoId().toString(), mensagem.getPayload())
                        .get(15, TimeUnit.SECONDS);
                mensagem.marcarPublicada();
                log.info("contrato.outbox.publicacao.sucesso contratoId={} eventId={} eventType={} partition={} offset={}",
                        mensagem.getContratoId(), mensagem.getEventId(), mensagem.getEventType(),
                        resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                span.error(e);
                log.error("contrato.outbox.publicacao.interrompida contratoId={} eventId={}",
                        mensagem.getContratoId(), mensagem.getEventId());
                return;
            } catch (Exception e) {
                span.error(e);
                log.error("contrato.outbox.publicacao.falha contratoId={} eventId={} eventType={} erro={}",
                        mensagem.getContratoId(), mensagem.getEventId(), mensagem.getEventType(), e.toString());
                return;
            } finally {
                span.end();
                MDC.remove("correlationId");
            }
        }
    }

    private Map<String, String> lerTraceContext(String json) {
        if (json == null || json.isBlank()) return Map.of();
        return mapper.readValue(json, new TypeReference<Map<String, String>>() {});
    }
}