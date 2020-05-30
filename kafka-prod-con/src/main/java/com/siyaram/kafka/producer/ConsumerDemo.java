package com.siyaram.kafka.producer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/* To successfully run consumer zookeeper,kafka server, kafka console producer should be up & running
 * */
public class ConsumerDemo 
{
	static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    public static void main( String[] args )
    {
    	String topic = "first_topic";    	    	
    	String groupId="my_first_application";
    	Properties prop = new Properties();
    	prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);    	
    	
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
    	consumer.subscribe(Arrays.asList(topic));
    	while(true) {
    		ConsumerRecords<String, String> records= consumer.poll(100);
    		for(ConsumerRecord<String, String> record:records) {
    			logger.info("Topic:"+record.topic());
    			logger.info("Partition:"+record.partition());
    			logger.info("Offset:"+record.offset());    			
    			logger.info("Key:"+record.key());
    			logger.info("Value:"+record.value());
    		}
    	}
    	
    	
    	
    }
}
