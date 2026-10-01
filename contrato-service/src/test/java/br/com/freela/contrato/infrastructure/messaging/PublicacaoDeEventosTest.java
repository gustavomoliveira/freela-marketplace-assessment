package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import br.com.freela.contrato.infrastructure.persistence.OutboxJpaEntity;
import br.com.freela.contrato.infrastructure.persistence.SpringDataOutboxRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = "eureka.client.enabled=false")
@Testcontainers
class PublicacaoDeEventosTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.1");

    @Autowired
    ContratoApplicationService service;
    @Autowired
    SpringDataOutboxRepository outbox;
    @Autowired
    JsonMapper mapper;

    @AfterEach
    void limparMdc() {
        MDC.clear();
    }

    @Test
    void publicaEventosComChaveDoContratoNaMesmaParticaoEEmOrdem() {
        MDC.put("correlationId", "teste-it-publicacao");
        var contrato = service.criar(novoComando());
        service.registrarEntrega(contrato.id());
        service.concluir(contrato.id());

        List<ConsumerRecord<String, String>> registros = consumirDoTopico(contrato.id().toString(), 3);

        assertThat(registros).hasSize(3);
        assertThat(registros).extracting(ConsumerRecord::key).containsOnly(contrato.id().toString());
        assertThat(registros).extracting(ConsumerRecord::partition).containsOnly(registros.get(0).partition());
        assertThat(registros).extracting(r -> mapper.readTree(r.value()).path("eventType").asString())
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");

        JsonNode envelope = mapper.readTree(registros.get(0).value());
        assertThat(envelope.path("eventId").asString()).isNotBlank();
        assertThat(envelope.path("contratoId").asString()).isEqualTo(contrato.id().toString());
        assertThat(envelope.path("occurredAt").asString()).isNotBlank();
        assertThat(envelope.path("correlationId").asString()).isEqualTo("teste-it-publicacao");
        assertThat(envelope.path("payload").path("titulo").asString()).isEqualTo("API de pagamentos");

        assertThat(outbox.findTop50ByPublicadoEmIsNullOrderByIdAsc())
                .noneMatch(o -> o.getContratoId().equals(contrato.id()));
    }

    @Test
    void regraVioladaNaoGravaEventoNaOutbox() {
        var contrato = service.criar(novoComando());
        service.registrarEntrega(contrato.id());

        assertThatThrownBy(() -> service.cancelar(contrato.id())).isInstanceOf(IllegalStateException.class);

        assertThat(service.buscar(contrato.id()).status().name()).isEqualTo("ENTREGA_REGISTRADA");
        assertThat(outbox.findAll())
                .filteredOn(o -> o.getContratoId().equals(contrato.id()))
                .extracting(OutboxJpaEntity::getEventType)
                .containsExactlyInAnyOrder("ContratoCriado", "EntregaRegistrada");
    }

    private CriarContratoCommand novoComando() {
        return new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "API de pagamentos", new BigDecimal("3500.00"));
    }

    private List<ConsumerRecord<String, String>> consumirDoTopico(String chave, int quantidade) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "teste-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> encontrados = new ArrayList<>();
        long limite = System.currentTimeMillis() + 30_000;
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(Topicos.CONTRATO_EVENTOS));
            while (encontrados.size() < quantidade && System.currentTimeMillis() < limite) {
                for (var registro : consumer.poll(Duration.ofMillis(500))) {
                    if (chave.equals(registro.key())) encontrados.add(registro);
                }
            }
        }
        return encontrados;
    }
}
