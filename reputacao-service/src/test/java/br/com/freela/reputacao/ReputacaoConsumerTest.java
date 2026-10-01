package br.com.freela.reputacao;

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
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = "eureka.client.enabled=false")
@Testcontainers
class ReputacaoConsumerTest {

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
    ReputacaoRepository repository;
    @Autowired
    EventoProcessadoRepository processados;

    @Test
    void mensagemDuplicadaNaoIncrementaAReputacaoDuasVezes() {
        UUID contratoId = UUID.randomUUID();
        UUID freelancerId = UUID.randomUUID();
        UUID eventoRepetido = UUID.randomUUID();
        UUID eventoSeguinte = UUID.randomUUID();
        String chave = contratoId.toString();

        kafkaTemplate.send("contrato.eventos", chave, concluido(eventoRepetido, contratoId, freelancerId, "100.00"));
        kafkaTemplate.send("contrato.eventos", chave, concluido(eventoRepetido, contratoId, freelancerId, "100.00"));
        kafkaTemplate.send("contrato.eventos", chave, concluido(eventoSeguinte, contratoId, freelancerId, "50.00"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(processados.existsById(eventoSeguinte)).isTrue());

        var reputacao = repository.findById(freelancerId).orElseThrow();
        assertThat(reputacao.getContratosConcluidos()).isEqualTo(2);
        assertThat(reputacao.getValorTotal()).isEqualByComparingTo(new BigDecimal("150"));
    }

    private String concluido(UUID eventId, UUID contratoId, UUID freelancerId, String valor) {
        return """
                {"eventId":"%s","eventType":"ContratoConcluido","contratoId":"%s","occurredAt":"2026-10-01T12:00:00Z","correlationId":"teste-it",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"Teste","valor":%s}}
                """.formatted(eventId, contratoId, UUID.randomUUID(), freelancerId, valor);
    }
}
