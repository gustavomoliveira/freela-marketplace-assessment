package br.com.freela.auditoria;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "auditoria_eventos")
public class EventoAuditoria {
    @Id
    private UUID id;
    @Column(unique = true)
    private UUID eventId;
    private UUID aggregateId;
    private String eventType;
    private String correlationId;
    private Instant occurredAt;
    @Column(columnDefinition = "text")
    private String payload;
    private Instant recebidoEm;

    protected EventoAuditoria() {}

    EventoAuditoria(UUID eventId, UUID aggregateId, String eventType, String correlationId,
                    Instant occurredAt, String payload) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.correlationId = correlationId;
        this.occurredAt = occurredAt;
        this.payload = payload;
        this.recebidoEm = Instant.now();
    }

    public UUID getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getCorrelationId() { return correlationId; }
    public Instant getOccurredAt() { return occurredAt; }
    public String getPayload() { return payload; }
    public Instant getRecebidoEm() { return recebidoEm; }
}