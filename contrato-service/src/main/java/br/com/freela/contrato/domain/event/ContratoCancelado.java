package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(UUID eventId, Instant occurredAt, UUID contratoId,
                                UUID clienteId, UUID freelancerId, String titulo)
        implements DomainEvent {

    public static ContratoCancelado novo(Contrato c) {
        return new ContratoCancelado(UUID.randomUUID(), Instant.now(), c.id(),
                c.clienteId(), c.freelancerId(), c.titulo());
    }

    @Override public String eventType() { return "ContratoCancelado"; }
}