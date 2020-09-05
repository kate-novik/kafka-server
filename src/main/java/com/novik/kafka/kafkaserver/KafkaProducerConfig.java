package com.novik.kafka.kafkaserver;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import com.novik.kafka.kafkaserver.dto.StarshipDto;

/**
 * Configuration class for Kafka Producer.
 *
 * @author Kate Novik
 */
@Configuration
public class KafkaProducerConfig {

  @Value("${kafka.server}")
  private String kafkaServer;

  @Value("${kafka.producer.id}")
  private String kafkaProducerId;

  /**
   * Create Kafka Template bean for message in topic.
   * Note: you should create own bean for every type of message in topic.
   * @return KafkaTemplate{@literal<Long, StarshipDto>}
   */
  @Bean
  public KafkaTemplate<Long, StarshipDto> kafkaTemplate() {
    KafkaTemplate<Long, StarshipDto> template = new KafkaTemplate<>(producerStarshipFactory());
    template.setMessageConverter(new StringJsonMessageConverter());
    return template;
  }

  /**
   * Create Kafka Producer Factory bean.
   * Note: you should create own bean for every type of message in topic.
   * @return ProducerFactory{@literal<Long, StarshipDto>}
   */
  @Bean
  public ProducerFactory<Long, StarshipDto> producerStarshipFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  /**
   * Create bean of producer's configuration data.
   * @return Map{@literal<String, Object>}
   */
  @Bean
  public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProducerId);
    return props;
  }
}
