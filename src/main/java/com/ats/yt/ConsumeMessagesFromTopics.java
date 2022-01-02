/**
 * 
 */
package com.ats.yt;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author suhail
 *
 */
public class ConsumeMessagesFromTopics {

	
	public static void main(String[] args) {
		
		String brokers = "glider-01.srvs.cloudkafka.com:9094,glider-02.srvs.cloudkafka.com:9094,glider-03.srvs.cloudkafka.com:9094";
		String topic = "kp4zbdyu-TopicProd";
		String topic2 = "kp4zbdyu-TopicQE";
		String username = "kp4zbdyu";
		String password = "smqj_iUAqmA0iLG9g5p9V59bFuXj8e9t";
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", brokers);
		
		//converts bytes to object
		props.setProperty("key.deserializer", StringDeserializer.class.getName());
		props.setProperty("value.deserializer", StringDeserializer.class.getName());
		
		//Handle authentication
		props.setProperty("security.protocol", "SASL_SSL");
		props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
		props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""+username+"\" password=\""+password+"\";");
		
		props.setProperty("group.id", "myconsumer");
		
		//Create consumer object using KafkaConsumer class
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
		
		//from which topic you have to consume
		consumer.subscribe(Arrays.asList(topic,topic2));
		
		while(true) {
			ConsumerRecords<String,String> records = consumer.poll(100);
			for(ConsumerRecord<String,String> record:records) {
				System.out.println(record.value());
			}
		}

	}

}
