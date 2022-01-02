package com.ats.yt;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SendMessageToTopic {

	public static void main(String[] args) {
		
		String brokers = "glider-01.srvs.cloudkafka.com:9094,glider-02.srvs.cloudkafka.com:9094,glider-03.srvs.cloudkafka.com:9094";
		String topic = "kp4zbdyu-TopicProd";
		String topic2 = "9ir1smec-StgTopic";
		String username = "kp4zbdyu";
		String password = "smqj_iUAqmA0iLG9g5p9V59bFuXj8e9t";
		
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", brokers);
		
		//converts objects to bytes
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", StringSerializer.class.getName());
		
		//Handle authentication
		props.setProperty("security.protocol", "SASL_SSL");
		props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
		props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""+username+"\" password=\""+password+"\";");
		
		KafkaProducer<String,String> myproducer = new KafkaProducer<>(props);
		
		//ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic2, "process this message");
		
		ProducerRecord<String,String> recordkv = new ProducerRecord<String, String>(topic, "ats_key5","ats_value5");
		
		//ProducerRecord<String,String> recordp = new ProducerRecord<String, String>(topic, 2,"ats_key4","ats_value4");
		
		//ProducerRecord<String,String> recordtopic2 = new ProducerRecord<String, String>(topic2, 3,"ats_key","ats_value");
		
		//myproducer.send(record);
		
		myproducer.send(recordkv);
		
		//myproducer.send(recordp);
		
		//myproducer.send(recordtopic2);
		
		myproducer.flush();
		
		myproducer.close();
		

	}

}
