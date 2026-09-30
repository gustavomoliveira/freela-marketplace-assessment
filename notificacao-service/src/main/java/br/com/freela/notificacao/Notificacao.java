package br.com.freela.notificacao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notificacoes")
public class Notificacao {
    @Id
    private UUID id;
    @Column(unique = true)
    private UUID eventId;
    private UUID contratoId;
    private UUID destinatarioId;
    private String tipo;
    @Column(length = 500)
    private String mensagem;
    private Instant criadaEm;

    protected Notificacao() {}

    Notificacao(UUID eventId, UUID contratoId, UUID destinatarioId, String tipo, String mensagem) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.contratoId = contratoId;
        this.destinatarioId = destinatarioId;
        this.tipo = tipo;
        this.mensagem = mensagem;
        this.criadaEm = Instant.now();
    }

    public UUID getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getContratoId() { return contratoId; }
    public UUID getDestinatarioId() { return destinatarioId; }
    public String getTipo() { return tipo; }
    public String getMensagem() { return mensagem; }
    public Instant getCriadaEm() { return criadaEm; }
}