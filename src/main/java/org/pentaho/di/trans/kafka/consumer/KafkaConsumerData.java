package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.time.Instant;

/**
 * Holds data processed by this step
 *
 * @author Michael
 * @author Miguel Ángel García
 */
public class KafkaConsumerData extends BaseStepData implements StepDataInterface
{
    // Kafka consumer.
    Consumer consumer;
    // Total messages read from a topic.
    int totalMessages;
    // Maximum messages to read.
    int maxMessages;
    // Maximum time to read.
    long maxTime;
    Instant startedOn;
    // Output row meta.
    RowMetaInterface outputRowMeta;
}
