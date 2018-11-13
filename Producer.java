package com.kafka.demo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;

public class Producer {
 
	public static void main(String args[]) {
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader("agent_not_ready_detail.json");
			br = new BufferedReader(fr);

			String sCurrentLine;
			StringBuilder data = new StringBuilder();
			Gson gs = new Gson();
			

			while ((sCurrentLine = br.readLine()) != null) {
				data.append(sCurrentLine);
				data.append(System.lineSeparator());
			}
			System.out.println(data.toString());
//			Market market = gs.fromJson(data.toString(), Market.class);
//			System.out.println("data is "+ market.getMarket());
//			
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            LongSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
producer.send(new ProducerRecord<String, String>("test", data.toString()));
producer.flush();

	}catch(Exception e) {
		e.printStackTrace();
	}
	}
}
