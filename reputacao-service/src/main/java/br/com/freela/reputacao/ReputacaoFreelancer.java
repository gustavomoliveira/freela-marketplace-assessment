package br.com.freela.reputacao;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "reputacoes")
public class ReputacaoFreelancer {
    @Id
    private UUID freelancerId;
    private int contratosConcluidos;
    private BigDecimal valorTotal = BigDecimal.ZERO;

    protected ReputacaoFreelancer() {}

    ReputacaoFreelancer(UUID id) {
        this.freelancerId = id;
    }

    void registrarContrato(BigDecimal valor) {
        contratosConcluidos++;
        valorTotal = valorTotal.add(valor);
    }

    public UUID getFreelancerId() { return freelancerId; }
    public int getContratosConcluidos() { return contratosConcluidos; }
    public BigDecimal getValorTotal() { return valorTotal; }
}