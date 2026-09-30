package br.com.freela.contrato.domain.repository;
import br.com.freela.contrato.domain.shared.DomainEvent;
public interface OutboxRepository {
    void registrar(DomainEvent evento);
}