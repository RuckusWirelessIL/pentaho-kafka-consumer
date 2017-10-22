package org.pentaho.di.trans.kafka.consumer;

import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;
import org.pentaho.di.core.exception.KettleException;

import java.util.concurrent.Callable;

/**
 * Kafka reader callable
 *
 * @author Michael Spector
 */
public abstract class KafkaConsumerCallable implements Callable<Object> {

    private KafkaConsumerData data;
    private KafkaConsumerMeta meta;
    private KafkaConsumer step;

    public KafkaConsumerCallable(KafkaConsumerMeta meta, KafkaConsumerData data, KafkaConsumer step) {
        this.meta = meta;
        this.data = data;
        this.step = step;
    }

    /**
     * Called when new message arrives from Kafka stream
     *
     * @param message Kafka message
     * @param key     Kafka key
     */
    protected abstract void messageReceived(byte[] key, byte[] message) throws KettleException;

    public Object call() throws KettleException {
        try {
            long limit;
            String strData = meta.getLimit();

            limit = getLimit(strData);
            if (limit > 0) {
                step.logDebug("Collecting up to " + limit + " messages");
            } else {
                step.logDebug("Collecting unlimited messages");
            }
            while (data.streamIterator.hasNext() && !data.canceled && (limit <= 0 || data.processed < limit)) {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = data.streamIterator.next();
                messageReceived(messageAndMetadata.key(), messageAndMetadata.message());
                ++data.processed;
            }
        } catch (ConsumerTimeoutException cte) {
            step.logDebug("Received a consumer timeout after " + data.processed + " messages");
            if (!meta.isStopOnEmptyTopic()) {
                // Because we're not set to stop on empty, this is an abnormal
                // timeout
                throw new KettleException("Unexpected consumer timeout!", cte);
            }
        }
        // Notify that all messages were read successfully
        data.consumer.commitOffsets();
        step.setOutputDone();
        return null;
    }

    private long getLimit(String strData) throws KettleException {
        long limit;
        try {
            limit = KafkaConsumerMeta.isEmpty(strData) ? 0 : Long.parseLong(step.environmentSubstitute(strData));
        } catch (NumberFormatException e) {
            throw new KettleException("Unable to parse messages limit parameter", e);
        }
        return limit;
    }
}
