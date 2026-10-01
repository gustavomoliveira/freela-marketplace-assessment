package br.com.freela.auditoria;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = "eureka.client.enabled=false")
@Testcontainers
class AuditoriaConsumerTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.1");

    @TestConfiguration
    static class TopicoConfig {
        @Bean
        NewTopic contratoEventos() {
            return TopicBuilder.name("contrato.eventos").partitions(3).replicas(1).build();
        }
    }

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    EventoAuditoriaRepository repository;

    @Test
    void mensagemDuplicadaNaoGeraRegistroRepetidoNaAuditoria() {
        UUID contratoId = UUID.randomUUID();
        UUID eventoRepetido = UUID.randomUUID();
        UUID eventoSeguinte = UUID.randomUUID();

        enviar(contratoId, eventoRepetido, "ContratoCriado");
        enviar(contratoId, eventoRepetido, "ContratoCriado");
        enviar(contratoId, eventoSeguinte, "EntregaRegistrada");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(repository.existsByEventId(eventoSeguinte)).isTrue());

        long registros = repository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(contratoId))
                .count();
        assertThat(registros).isEqualTo(2);
    }

    @Test
    void eventosDeUmMesmoContratoSaoRegistradosEmOrdemMesmoComConsumoConcorrente() {
        List<UUID> contratos = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        List<String> sequencia = List.of("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");

        for (String tipo : sequencia) {
            for (UUID contratoId : contratos) {
                enviar(contratoId, UUID.randomUUID(), tipo);
            }
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(repository.findAll())
                        .filteredOn(e -> contratos.contains(e.getAggregateId()))
                        .hasSize(9));

        for (UUID contratoId : contratos) {
            List<String> tipos = repository.findAll().stream()
                    .filter(e -> e.getAggregateId().equals(contratoId))
                    .sorted(Comparator.comparing(EventoAuditoria::getRecebidoEm))
                    .map(EventoAuditoria::getEventType)
                    .toList();
            assertThat(tipos).containsExactlyElementsOf(sequencia);
        }
    }

    private void enviar(UUID contratoId, UUID eventId, String tipo) {
        kafkaTemplate.send("contrato.eventos", contratoId.toString(), mensagem(eventId, tipo, contratoId));
    }

    private String mensagem(UUID eventId, String tipo, UUID contratoId) {
        return """
                {"eventId":"%s","eventType":"%s","contratoId":"%s","occurredAt":"2026-10-01T12:00:00Z","correlationId":"teste-it",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"Teste","valor":100.00}}
                """.formatted(eventId, tipo, contratoId, UUID.randomUUID(), UUID.randomUUID());
    }
}
