package br.com.freela.reputacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;

@Service
public class ReputacaoService {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoService.class);
    private final ReputacaoRepository repository;
    private final EventoProcessadoRepository processados;

    public ReputacaoService(ReputacaoRepository repository, EventoProcessadoRepository processados) {
        this.repository = repository;
        this.processados = processados;
    }

    @Transactional
    public void registrarContratoConcluido(UUID eventId, UUID contratoId, UUID freelancerId, BigDecimal valor) {
        log.info("reputacao.atualizacao.inicio eventId={} contratoId={} freelancerId={}",
                eventId, contratoId, freelancerId);

        if (processados.existsById(eventId)) {
            log.warn("reputacao.atualizacao.duplicado eventId={} contratoId={} freelancerId={}",
                    eventId, contratoId, freelancerId);
            return;
        }

        var reputacao = repository.findById(freelancerId)
                .orElseGet(() -> new ReputacaoFreelancer(freelancerId));
        reputacao.registrarContrato(valor);
        repository.save(reputacao);
        processados.save(new EventoProcessado(eventId));

        log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} contratosConcluidos={}",
                eventId, contratoId, freelancerId, reputacao.getContratosConcluidos());
    }
}