package com.ruckuswireless.pentaho.kafka.consumer;

import kafka.consumer.ConsumerTimeoutException;

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
	private KafkaConsumerStep step;

	public KafkaConsumerCallable(KafkaConsumerMeta meta, KafkaConsumerData data, KafkaConsumerStep step) {
		this.meta = meta;
		this.data = data;
		this.step = step;
	}

	/**
	 * Called when new message arrives from Kafka stream
	 *
	 * @param message
	 *            Kafka message
	 */
	protected abstract void messageReceived(byte[] message) throws KettleException;

	public Object call() throws KettleException {
		try {
			long limit = meta.getLimit();
			if (limit > 0) {
				step.logDebug("Collecting up to " + limit + " messages");
			} else {
				step.logDebug("Collecting unlimited messages");
			}
			while (data.streamIterator.hasNext() && !data.canceled && (limit <= 0 || data.processed < limit)) {
				messageReceived(data.streamIterator.next().message());
				++data.processed;
			}
		} catch (ConsumerTimeoutException cte) {
			step.logDebug("Received a consumer timeout after " + data.processed + " messages");
			if (!meta.isStopOnEmptyTopic()) {
				// Because we're not set to stop on empty, this is an abnormal timeout
				throw new KettleException("Unexpected consumer timeout!", cte);
			}
		}
		// Notify that all messages were read successfully
		data.consumer.commitOffsets();
		step.setOutputDone();
		return null;
	}
}
