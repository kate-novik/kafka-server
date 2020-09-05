package com.novik.kafka.kafkaserver;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

/**
 * Configuration class for Kafka Consumer
 *
 * @author Kate Novik
 */
@Configuration
public class KafkaConsumerConfig {

  @Value("${kafka.server}")
  private String kafkaServer;

  @Value("${kafka.group.id}")
  private String kafkaGroupId;

  /**
   * Create Kafka listeners container bean for reading single message from topic.
   * It is used a message converter with JSON output.
   * @return KafkaListenerContainerFactory
   */
  @Bean
  public KafkaListenerContainerFactory<?> singleFactory() {
    ConcurrentKafkaListenerContainerFactory<Long, AbstractDto> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setBatchListener(false);
    factory.setMessageConverter(converter());
    return factory;
  }

  /**
   * Create Kafka listeners container bean for reading batch of messages from topic.
   * It is used a message converter with array of JSON output.
   * @return KafkaListenerContainerFactory
   */
  @Bean
  public KafkaListenerContainerFactory<?> batchFactory() {
    ConcurrentKafkaListenerContainerFactory<Long, AbstractDto> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setBatchListener(true);
    factory.setMessageConverter(new BatchMessagingMessageConverter(converter()));
    return factory;
  }

  @Bean
  public ConsumerFactory<Long, AbstractDto> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  /**
   * Create bean of consumer's configuration data.
   * @return Map{literal<String, Object>}
   */
  @Bean
  public Map<String, Object> consumerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    return props;
  }

  @Bean
  public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
    return new ConcurrentKafkaListenerContainerFactory<>();
  }

  /**
   * Create converter bean of topic's message in String format to JSON.
   * @return StringJsonMessageConverter
   */
  @Bean
  public StringJsonMessageConverter converter() {
    return new StringJsonMessageConverter();
  }
}
