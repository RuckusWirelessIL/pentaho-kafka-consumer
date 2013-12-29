package com.ruckuswireless.pentaho.kafka.consumer;

import java.util.concurrent.Callable;

import org.pentaho.di.core.exception.KettleException;

/**
 * Kafka reader callable
 * 
 * @author Michael Spector
 */
public abstract class KafkaConsumerCallable implements Callable<Object> {

	private KafkaConsumerData data;
	private KafkaConsumerMeta meta;

	public KafkaConsumerCallable(KafkaConsumerMeta meta, KafkaConsumerData data) {
		this.meta = meta;
		this.data = data;
	}

	/**
	 * Called when new message arrives from Kafka stream
	 * 
	 * @param message
	 *            Kafka message
	 */
	protected abstract void messageReceived(byte[] message) throws KettleException;

	public Object call() throws KettleException {
		long limit = meta.getLimit();
		while (data.streamIterator.hasNext() && !data.canceled && (limit <= 0 || data.processed < limit)) {
			messageReceived(data.streamIterator.next().message());
			++data.processed;
		}
		// Notify that all messages were read successfully
		data.consumer.commitOffsets();
		return null;
	}
}