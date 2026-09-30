package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.UUID;

@Service
public class AuditoriaService {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);
    private final EventoAuditoriaRepository repository;

    public AuditoriaService(EventoAuditoriaRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void registrar(UUID eventId, UUID aggregateId, String eventType, String correlationId,
                          Instant occurredAt, String payload) {
        log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={}",
                eventId, aggregateId, eventType, correlationId);

        if (repository.existsByEventId(eventId)) {
            log.warn("auditoria.registro.duplicado eventId={} aggregateId={} eventType={}",
                    eventId, aggregateId, eventType);
            return;
        }

        var evento = repository.save(new EventoAuditoria(eventId, aggregateId, eventType, correlationId, occurredAt, payload));
        log.info("auditoria.registro.sucesso auditoriaId={} eventId={} aggregateId={} eventType={}",
                evento.getId(), eventId, aggregateId, eventType);
    }
}