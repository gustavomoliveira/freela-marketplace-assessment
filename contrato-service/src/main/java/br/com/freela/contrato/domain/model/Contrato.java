package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Contrato {
    private final UUID id;
    private final UUID clienteId;
    private final UUID freelancerId;
    private final String titulo;
    private final BigDecimal valor;
    private StatusContrato status;
    private final Instant criadoEm;
    private final List<DomainEvent> domainEvents = new ArrayList<>();

    private Contrato(UUID id, UUID clienteId, UUID freelancerId, String titulo,
                     BigDecimal valor, StatusContrato status, Instant criadoEm) {
        this.id = id; this.clienteId = clienteId; this.freelancerId = freelancerId;
        this.titulo = titulo; this.valor = valor; this.status = status; this.criadoEm = criadoEm;
    }

    public static Contrato criar(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor) {
        if (clienteId == null || freelancerId == null) throw new IllegalArgumentException("Cliente e freelancer são obrigatórios");
        if (titulo == null || titulo.isBlank()) throw new IllegalArgumentException("Título é obrigatório");
        if (valor == null || valor.signum() <= 0) throw new IllegalArgumentException("Valor deve ser positivo");
        var contrato = new Contrato(UUID.randomUUID(), clienteId, freelancerId, titulo.trim(), valor,
                StatusContrato.ATIVO, Instant.now());
        contrato.domainEvents.add(ContratoCriado.novo(contrato));
        return contrato;
    }

    public static Contrato restaurar(UUID id, UUID clienteId, UUID freelancerId, String titulo,
                                     BigDecimal valor, StatusContrato status, Instant criadoEm) {
        return new Contrato(id, clienteId, freelancerId, titulo, valor, status, criadoEm);
    }

    public void registrarEntrega() {
        if (status != StatusContrato.ATIVO) throw new IllegalStateException("Somente contratos ativos recebem entrega");
        status = StatusContrato.ENTREGA_REGISTRADA;
        domainEvents.add(EntregaRegistrada.novo(this));
    }

    public void concluir() {
        if (status != StatusContrato.ENTREGA_REGISTRADA) throw new IllegalStateException("A entrega precisa estar registrada");
        status = StatusContrato.CONCLUIDO;
        domainEvents.add(ContratoConcluido.novo(this));
    }

    public void cancelar() {
        if (status != StatusContrato.ATIVO) throw new IllegalStateException("Somente contratos ativos podem ser cancelados");
        status = StatusContrato.CANCELADO;
        domainEvents.add(ContratoCancelado.novo(this));
    }

    public List<DomainEvent> pullDomainEvents() {
        var copy = List.copyOf(domainEvents); domainEvents.clear(); return copy;
    }
    public List<DomainEvent> domainEvents() { return Collections.unmodifiableList(domainEvents); }
    public UUID id(){return id;} public UUID clienteId(){return clienteId;} public UUID freelancerId(){return freelancerId;}
    public String titulo(){return titulo;} public BigDecimal valor(){return valor;} public StatusContrato status(){return status;}
    public Instant criadoEm(){return criadoEm;}
}