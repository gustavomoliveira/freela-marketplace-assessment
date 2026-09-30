package br.com.freela.contrato.infrastructure.persistence;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface SpringDataOutboxRepository extends JpaRepository<OutboxJpaEntity, Long> {
    List<OutboxJpaEntity> findTop50ByPublicadoEmIsNullOrderByIdAsc();
}