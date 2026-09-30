package br.com.freela.reputacao;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_processados")
class EventoProcessado {
    @Id
    private UUID eventId;
    private Instant processadoEm;

    protected EventoProcessado() {}

    EventoProcessado(UUID eventId) {
        this.eventId = eventId;
        this.processadoEm = Instant.now();
    }
}