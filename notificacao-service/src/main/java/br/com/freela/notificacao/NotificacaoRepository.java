package br.com.freela.notificacao;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

interface NotificacaoRepository extends JpaRepository<Notificacao, UUID> {
    boolean existsByEventId(UUID eventId);
}