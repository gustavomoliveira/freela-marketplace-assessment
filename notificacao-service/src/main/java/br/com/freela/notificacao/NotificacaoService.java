package br.com.freela.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Service
public class NotificacaoService {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoService.class);
    private final NotificacaoRepository repository;

    public NotificacaoService(NotificacaoRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void registrar(UUID eventId, UUID contratoId, UUID destinatarioId, String tipo, String mensagem) {
        log.info("notificacao.registro.inicio eventId={} contratoId={} destinatarioId={} tipo={}",
                eventId, contratoId, destinatarioId, tipo);

        if (repository.existsByEventId(eventId)) {
            log.warn("notificacao.registro.duplicado eventId={} contratoId={} destinatarioId={}",
                    eventId, contratoId, destinatarioId);
            return;
        }

        var notificacao = repository.save(new Notificacao(eventId, contratoId, destinatarioId, tipo, mensagem));
        log.info("notificacao.registro.sucesso notificacaoId={} eventId={} contratoId={} destinatarioId={}",
                notificacao.getId(), eventId, contratoId, destinatarioId);
    }
}