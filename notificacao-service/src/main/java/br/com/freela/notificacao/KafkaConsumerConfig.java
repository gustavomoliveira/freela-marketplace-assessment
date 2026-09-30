package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));

        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> registro, Exception ex, int tentativa) {
                log.warn("notificacao.consumo.falha contratoId={} topico={} partition={} offset={} tentativa={} erro={}",
                        registro.key(), registro.topic(), registro.partition(), registro.offset(), tentativa, ex.getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> registro, Exception ex) {
                log.error("notificacao.consumo.dlt contratoId={} topico={} partition={} offset={} erro={}",
                        registro.key(), registro.topic(), registro.partition(), registro.offset(), ex.getMessage());
            }
        });
        return handler;
    }
}