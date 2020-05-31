package com.siyaram.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Constants.FilterLevel;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterClient {
	Logger logger = LoggerFactory.getLogger(TwitterClient.class);

	public TwitterClient() {
	}

	public static void main(String[] args) {
		new TwitterClient().run();

	}

	public void run() {
		// connet to twitter
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client = connectTwitter(msgQueue);
		KafkaProducer<String, String> producer = createKafkaProducer();
		client.connect();		
		// send twitter message to kafka server
		String msg=null;
		while (!client.isDone()) {
			  try {
				  if(msgQueue.contains("kafka")) {
					  System.out.println("kafka word is available in message");
				  }
				msg = msgQueue.poll(5,TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				client.stop();
				logger.error("Error during message read:",e);
			}
		if(msg != null) {
			logger.info(msg);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", msg);
			producer.send(record);			
			logger.info("message sent successfully to kafka server....");
		}
		}
		logger.info("End of application...........!");
		}

	private KafkaProducer<String, String> createKafkaProducer() {
			
    	Properties prop = new Properties();
    	prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());    	    	
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
		return producer;
	}

	private static Client connectTwitter(BlockingQueue<String> msgQueue) {
		String consumerKey = "7c7oC3wH014n2sG2hP6QjrN7";
		String consumerSecret = "qLb7MeuLKRfwV2rTezUP291w4sHlwNzFR4xgDQPzhwd773FQ75";
		String token = "113650100-rrojoxKUc5NHO9N82kNGoiNNWUO36rCStgpPhkE";
		String tokenSecret = "1MxIHCkAvdZI5LbCYNqBHXZuHDpJbZhqgv971KIDQ2exr";
			
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("kafka");
		hosebirdEndpoint.trackTerms(terms);
		hosebirdEndpoint.filterLevel(FilterLevel.Low);								
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01").hosts(hosebirdHosts)
				.authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();		
		return hosebirdClient;
	}
}
