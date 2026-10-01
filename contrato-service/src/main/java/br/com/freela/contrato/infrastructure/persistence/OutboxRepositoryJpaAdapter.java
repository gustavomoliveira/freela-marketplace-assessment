package br.com.freela.contrato.infrastructure.persistence;

import br.com.freela.contrato.domain.repository.OutboxRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.messaging.EventoEnvelope;
import br.com.freela.contrato.infrastructure.messaging.Topicos;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;

@Repository
public class OutboxRepositoryJpaAdapter implements OutboxRepository {
    private static final Logger log = LoggerFactory.getLogger(OutboxRepositoryJpaAdapter.class);
    private final SpringDataOutboxRepository jpa;
    private final JsonMapper mapper;
    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxRepositoryJpaAdapter(SpringDataOutboxRepository jpa, JsonMapper mapper,
                                      Tracer tracer, Propagator propagator) {
        this.jpa = jpa;
        this.mapper = mapper;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(DomainEvent evento) {
        String correlationId = MDC.get("correlationId");
        log.info("contrato.outbox.registro.inicio contratoId={} eventId={} eventType={}",
                evento.contratoId(), evento.eventId(), evento.eventType());

        ObjectNode payload = mapper.valueToTree(evento);
        payload.remove("eventId");
        payload.remove("occurredAt");
        payload.remove("contratoId");
        payload.remove("eventType");

        var envelope = new EventoEnvelope(evento.eventId(), evento.eventType(), evento.contratoId(),
                evento.occurredAt(), correlationId, payload);
        jpa.save(new OutboxJpaEntity(evento.eventId(), evento.contratoId(), evento.eventType(),
                Topicos.CONTRATO_EVENTOS, correlationId, capturarTraceContext(),
                mapper.writeValueAsString(envelope)));

        log.info("contrato.outbox.registro.sucesso contratoId={} eventId={} eventType={}",
                evento.contratoId(), evento.eventId(), evento.eventType());
    }

    private String capturarTraceContext() {
        Map<String, String> carrier = new HashMap<>();
        Span span = tracer.currentSpan();
        if (span != null) {
            propagator.inject(span.context(), carrier, Map::put);
        }
        return mapper.writeValueAsString(carrier);
    }
}