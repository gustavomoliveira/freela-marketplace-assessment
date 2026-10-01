package br.com.freela.notificacao;

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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = "eureka.client.enabled=false")
@Testcontainers
class NotificacaoConsumerTest {

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
    NotificacaoRepository repository;

    @Test
    void mensagemDuplicadaNaoGeraNotificacaoRepetida() {
        UUID contratoId = UUID.randomUUID();
        UUID eventoRepetido = UUID.randomUUID();
        UUID eventoSeguinte = UUID.randomUUID();
        String chave = contratoId.toString();

        kafkaTemplate.send("contrato.eventos", chave, mensagem(eventoRepetido, "ContratoCriado", contratoId));
        kafkaTemplate.send("contrato.eventos", chave, mensagem(eventoRepetido, "ContratoCriado", contratoId));
        kafkaTemplate.send("contrato.eventos", chave, mensagem(eventoSeguinte, "EntregaRegistrada", contratoId));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(repository.existsByEventId(eventoSeguinte)).isTrue());

        long notificacoes = repository.findAll().stream()
                .filter(n -> n.getContratoId().equals(contratoId))
                .count();
        assertThat(notificacoes).isEqualTo(2);
    }

    private String mensagem(UUID eventId, String tipo, UUID contratoId) {
        return """
                {"eventId":"%s","eventType":"%s","contratoId":"%s","occurredAt":"2026-10-01T12:00:00Z","correlationId":"teste-it",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"Teste"}}
                """.formatted(eventId, tipo, contratoId, UUID.randomUUID(), UUID.randomUUID());
    }
}
