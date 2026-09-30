package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.infrastructure.persistence.SpringDataOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublicador {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublicador.class);
    private final SpringDataOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublicador(SpringDataOutboxRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${freela.outbox.intervalo-ms:1000}")
    @Transactional
    public void publicarPendentes() {
        var pendentes = repository.findTop50ByPublicadoEmIsNullOrderByIdAsc();

        if (pendentes.isEmpty()) return;

        log.info("contrato.outbox.publicacao.lote quantidade={}", pendentes.size());

        for (var mensagem : pendentes) {
            log.info("contrato.outbox.publicacao.inicio contratoId={} eventId={} eventType={} topico={} correlationId={}",
                    mensagem.getContratoId(), mensagem.getEventId(), mensagem.getEventType(), mensagem.getTopico(), mensagem.getCorrelationId());
            try {
                var resultado = kafkaTemplate.send(mensagem.getTopico(), mensagem.getContratoId().toString(), mensagem.getPayload())
                        .get(15, TimeUnit.SECONDS);
                mensagem.marcarPublicada();
                log.info("contrato.outbox.publicacao.sucesso contratoId={} eventId={} eventType={} partition={} offset={} correlationId={}",
                        mensagem.getContratoId(), mensagem.getEventId(), mensagem.getEventType(),
                        resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset(), mensagem.getCorrelationId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("contrato.outbox.publicacao.interrompida contratoId={} eventId={}", mensagem.getContratoId(), mensagem.getEventId());
                return;
            } catch (Exception e) {
                log.error("contrato.outbox.publicacao.falha contratoId={} eventId={} eventType={} correlationId={} erro={}",
                        mensagem.getContratoId(), mensagem.getEventId(), mensagem.getEventType(), mensagem.getCorrelationId(), e.toString());
                return;
            }
        }
    }
}