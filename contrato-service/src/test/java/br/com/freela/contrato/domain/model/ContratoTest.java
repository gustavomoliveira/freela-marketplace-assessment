package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.DomainEvent;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContratoTest {

    private Contrato novoContrato() {
        return Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API de pagamentos", new BigDecimal("3500.00"));
    }

    @Test
    void criarGeraEventoContratoCriado() {
        var contrato = novoContrato();

        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
        assertThat(contrato.domainEvents()).extracting(DomainEvent::eventType).containsExactly("ContratoCriado");
        assertThat(contrato.domainEvents().get(0).contratoId()).isEqualTo(contrato.id());
    }

    @Test
    void cicloCompletoGeraEventosNaOrdemDasTransicoes() {
        var contrato = novoContrato();

        contrato.registrarEntrega();
        contrato.concluir();

        assertThat(contrato.status()).isEqualTo(StatusContrato.CONCLUIDO);
        assertThat(contrato.domainEvents()).extracting(DomainEvent::eventType)
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
    }

    @Test
    void cadaEventoTemIdentificadorUnico() {
        var contrato = novoContrato();

        contrato.registrarEntrega();
        contrato.concluir();

        assertThat(contrato.domainEvents()).extracting(DomainEvent::eventId).doesNotHaveDuplicates();
    }

    @Test
    void cancelarContratoAtivoGeraContratoCancelado() {
        var contrato = novoContrato();

        contrato.cancelar();

        assertThat(contrato.status()).isEqualTo(StatusContrato.CANCELADO);
        assertThat(contrato.domainEvents()).extracting(DomainEvent::eventType)
                .containsExactly("ContratoCriado", "ContratoCancelado");
    }

    @Test
    void cancelarDepoisDaEntregaNaoEPermitidoENaoGeraEvento() {
        var contrato = novoContrato();
        contrato.registrarEntrega();

        assertThatThrownBy(contrato::cancelar).isInstanceOf(IllegalStateException.class);

        assertThat(contrato.status()).isEqualTo(StatusContrato.ENTREGA_REGISTRADA);
        assertThat(contrato.domainEvents()).hasSize(2);
    }

    @Test
    void concluirSemEntregaNaoEPermitido() {
        var contrato = novoContrato();

        assertThatThrownBy(contrato::concluir).isInstanceOf(IllegalStateException.class);

        assertThat(contrato.domainEvents()).hasSize(1);
    }

    @Test
    void pullDomainEventsEntregaOsEventosEEsvaziaALista() {
        var contrato = novoContrato();

        assertThat(contrato.pullDomainEvents()).hasSize(1);
        assertThat(contrato.domainEvents()).isEmpty();
    }

    @Test
    void criarComValorInvalidoFalha() {
        assertThatThrownBy(() -> Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API", BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
