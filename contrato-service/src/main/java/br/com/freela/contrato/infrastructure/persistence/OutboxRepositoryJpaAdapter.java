package br.com.freela.contrato.infrastructure.persistence;

import br.com.freela.contrato.domain.repository.OutboxRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.messaging.EventoEnvelope;
import br.com.freela.contrato.infrastructure.messaging.Topicos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

@Repository
public class OutboxRepositoryJpaAdapter implements OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(OutboxRepositoryJpaAdapter.class);
    private final SpringDataOutboxRepository jpa;
    private final JsonMapper mapper;

    public OutboxRepositoryJpaAdapter(SpringDataOutboxRepository jpa, JsonMapper mapper) {
        this.jpa = jpa;
        this.mapper = mapper;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(DomainEvent evento) {
        String correlationId = MDC.get("correlationId");

        log.info("contrato.outbox.registro.inicio contratoId={} eventId={} eventType={} correlationId={}",
                evento.contratoId(), evento.eventId(), evento.eventType(), correlationId);

        ObjectNode payload = mapper.valueToTree(evento);

        payload.remove("eventId"); payload.remove("occurredAt"); payload.remove("contratoId"); payload.remove("eventType");

        var envelope = new EventoEnvelope(evento.eventId(), evento.eventType(), evento.contratoId(),
                evento.occurredAt(), correlationId, payload);

        jpa.save(new OutboxJpaEntity(evento.eventId(), evento.contratoId(), evento.eventType(),
                Topicos.CONTRATO_EVENTOS, correlationId, mapper.writeValueAsString(envelope)));

        log.info("contrato.outbox.registro.sucesso contratoId={} eventId={} eventType={} correlationId={}",
                evento.contratoId(), evento.eventId(), evento.eventType(), correlationId);
    }
}