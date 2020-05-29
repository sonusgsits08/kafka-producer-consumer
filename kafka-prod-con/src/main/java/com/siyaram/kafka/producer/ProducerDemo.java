package com.siyaram.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo 
{
	static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main( String[] args )
    {
    	String topic = "first_topic";
    	String message="Hello World from java program";
    	Properties prop = new Properties();
    	prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
    	
    	for(int i=0;i<10;i++) {
    		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message+":"+i);	
        	producer.send(record,new Callback() {    			
    			public void onCompletion(RecordMetadata metadata, Exception exception) {
    				if(exception ==null) {
    					logger.info("Topic:"+metadata.topic());
    					logger.info("Partition:"+metadata.partition());
    					logger.info("Offset:"+metadata.offset());	
    				}
    				else {
    					logger.error("there is error in sending message:",exception);
    				}
    			}
    		});           	
    		
    	}
    	producer.close();
    }
}
