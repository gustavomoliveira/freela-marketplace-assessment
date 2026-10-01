package br.com.freela.contrato.infrastructure.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_eventos")
public class OutboxJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false, unique = true)
    private UUID eventId;
    @Column(nullable = false)
    private UUID contratoId;
    @Column(nullable = false, length = 80)
    private String eventType;
    @Column(nullable = false, length = 120)
    private String topico;
    @Column(length = 100)
    private String correlationId;
    @Column(columnDefinition = "text")
    private String traceContext;
    @Column(nullable = false, columnDefinition = "text")
    private String payload;
    @Column(nullable = false)
    private Instant criadoEm;
    private Instant publicadoEm;

    protected OutboxJpaEntity() {}

    public OutboxJpaEntity(UUID eventId, UUID contratoId, String eventType, String topico,
                           String correlationId, String traceContext, String payload) {
        this.eventId = eventId;
        this.contratoId = contratoId;
        this.eventType = eventType;
        this.topico = topico;
        this.correlationId = correlationId;
        this.traceContext = traceContext;
        this.payload = payload;
        this.criadoEm = Instant.now();
    }

    public void marcarPublicada() { this.publicadoEm = Instant.now(); }

    public Long getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getContratoId() { return contratoId; }
    public String getEventType() { return eventType; }
    public String getTopico() { return topico; }
    public String getCorrelationId() { return correlationId; }
    public String getTraceContext() { return traceContext; }
    public String getPayload() { return payload; }
    public Instant getCriadoEm() { return criadoEm; }
    public Instant getPublicadoEm() { return publicadoEm; }
}