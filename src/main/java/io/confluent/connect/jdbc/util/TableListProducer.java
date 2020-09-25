package io.confluent.connect.jdbc.util;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TableListProducer {
  Producer<String, String> producer;

  public TableListProducer(String brokerUrl) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", brokerUrl);
    properties.put("key.serializer", 
        "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", 
        "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<String, String>(properties);
  }
    
  public void produce(String topic, String key, String value) {
    this.producer.send(new ProducerRecord<String, String>(topic, key, value));            
  }
   
  public void close() {
    this.producer.close();
  }
}
