package com.kafka.demo;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {

	public static void main(String[] args) {
		final Properties props = new Properties();
	      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	                                  "localhost:9092");
	      props.put(ConsumerConfig.GROUP_ID_CONFIG,
	                                  "KafkaExampleConsumer");
	      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	              LongDeserializer.class.getName());
	      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	              StringDeserializer.class.getName());
	      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

	      // Create the consumer using props.
	      final KafkaConsumer<Long, String> consumer =
	                                  new KafkaConsumer<>(props);

	      // Subscribe to the topic.
	      consumer.subscribe(Collections.singletonList("test"));
//	      TopicPartition partition = new TopicPartition("test", 0);
//	      consumer.seekToBeginning(partition);
	      
	      final int giveUp = 100;   int noRecordsCount = 0;
	      
	        while (true) {
	            final ConsumerRecords<Long, String> consumerRecords =
	                    consumer.poll(1000);
	            if (consumerRecords.count()==0) {
	                noRecordsCount++;
	                if (noRecordsCount > giveUp) break;
	                else continue;
	            }
	            consumerRecords.forEach(record -> {
	                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
	                        record.key(), record.value(),
	                        record.partition(), record.offset());
	            });
	            consumer.commitAsync();
	        }
	        consumer.close();
	        System.out.println("DONE");

	}

}
