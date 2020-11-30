package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Kafka Consumer step processor
 *
 * @author Michael Spector
 * @author Miguel Ángel García
 */
public class KafkaConsumer extends BaseStep implements StepInterface
{
    private KafkaConsumerMeta meta;
    private KafkaConsumerData data;


    /**
     * Default step constructor.
     */
    public KafkaConsumer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
    {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }


    /**
     * Finalize step execution.
     * Close kafka consumer.
     *
     * @param  smi  StepMetaInterface
     * @param  sdi  StepDataInterface
     */
    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        this.meta = (KafkaConsumerMeta) smi;
        this.data = (KafkaConsumerData) sdi;
        // Close consumer.
        if (data.consumer != null) {
            data.consumer.close();
        }
        super.dispose(smi, sdi);
    }

    /**
     * User stops execution.
     * Close kafka consumer.
     *
     * @param  smi              StepMetaInterface
     * @param  sdi              StepDataInterface
     * @throws KettleException  When stopRunning fails.
     */
    @Override
    public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
    {
        this.meta = (KafkaConsumerMeta) smi;
        this.data = (KafkaConsumerData) sdi;
        // Close kafka consumer.
        this.data.consumer.close();
        // Stop step.
        super.stopRunning(smi, sdi);
    }

    /**
     * Init step.
     * Connect to Kafka and subscribe to a topic.
     *
     * @param  smi  Step Meta interface.
     * @param  sdi  Step Data interface.
     * @return      True when init is Ok, false when KO.
     */
    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi)
    {
        // Basic step init.
        super.init(smi, sdi);
        this.meta = (KafkaConsumerMeta) smi;
        this.data = (KafkaConsumerData) sdi;

        // Parse behaviours vars.
        String topic = environmentSubstitute(this.meta.getTopic());
        // Parse config props.
        Properties parsedProps = new Properties();
        for (Entry<Object, Object> e : this.meta.getKafkaProperties().entrySet()) {
            if (!"(default)".equals(e.getValue().toString())) {
                parsedProps.put(e.getKey(), environmentSubstitute(e.getValue().toString()));
            }
        }

        // Create kafka consumer and subscribe to a topic.
        log.logDebug(parsedProps.toString());

        // Avoid ClassLoader error when loading StringDeserialized.
        Thread currentThread = Thread.currentThread();
        ClassLoader savedClassLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(org.apache.kafka.clients.consumer.KafkaConsumer.class.getClassLoader());
        // Create Kafka Consumer.
        this.data.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(parsedProps);
        // Restore ClassLoader context.
        currentThread.setContextClassLoader(savedClassLoader);

        //TestConsumerRebalanceListener rebalanceListener = new TestConsumerRebalanceListener();
        //data.consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
        // Subscribe to a topic.
        this.data.consumer.subscribe(Collections.singletonList(topic));

        // Init time and message counters.
        this.data.totalMessages = 0;
        this.data.startedOn = Instant.now();

        // Init OK.
        return true;
    }

    /**
     * Method that will be called in a loop to generate and consume rows.
     * Kafka consumer does not expect any incoming row from previous steps.
     * It is designed to execute processRow() exactly once, fetching data from the outside world,
     * and putting them into the row stream by calling putRow() repeatedly until done.
     */
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
    {
        this.meta = (KafkaConsumerMeta) smi;
        this.data = (KafkaConsumerData) sdi;

        // Get the input row. Null on our case.
        Object[] r = getRow();

        // Row structure only the first time.
        if (this.first) {
            this.first = false;
            this.data.outputRowMeta = new RowMeta();
            this.meta.getFields(this.data.outputRowMeta, this.getStepname(), null, null, this, this.repository, this.metaStore);
            // Convert integer and time metas.
            this.data.maxMessages = Integer.parseInt(environmentSubstitute(this.meta.getLimit()));
            this.data.maxTime = Long.parseLong(environmentSubstitute(this.meta.getTimeout()));
            log.logDebug("Max messages set to " + this.data.maxMessages + " and Time Out set to " + this.data.maxTime + " ms.");
        }

        // Poll messages from topic.
        logDebug("Starting topic polling...");
        ConsumerRecords<byte[], byte[]> messages = this.data.consumer.poll(10000);

        // If no messages and stop when empty is set, we have finished.
        if (this.meta.isStopOnEmptyTopic() && messages.count() == 0) {
            logDebug("No more messages to read.");
            setOutputDone();
            return false;
        }

        // Output the messages.
        log.logDebug("Poll " + messages.count() + " messages from topic.");
        messages.forEach(msg -> {
            // Create new row.
            Object[] outputRow = RowDataUtil.allocateRowData(this.data.outputRowMeta.size());
            outputRow[0] = msg.key();
            outputRow[1] = msg.value();
            // Output the row.
            try {
                this.putRow(this.data.outputRowMeta, this.data.outputRowMeta.cloneRow(outputRow));
                log.logRowlevel(Messages.getString("KafkaConsumer.Log.OutputRow", Long.toString(getLinesWritten()), this.data.outputRowMeta.getString(outputRow)));
            } catch (KettleStepException | KettleValueException e) {
                e.printStackTrace();
            }
        });

        // Update read messages.
        this.data.totalMessages += messages.count();

        // Check total messages read.
        if ((this.data.maxMessages > 0) && (this.data.totalMessages >= this.data.maxMessages)) {
            logDebug("Reached maximum messages to read (" + this.data.totalMessages + ").");
            setOutputDone();
            return false;
        }

        // Check time reading.
        if ((this.data.maxTime > 0) && (ChronoUnit.MILLIS.between(this.data.startedOn, Instant.now()) > this.data.maxTime)) {
            logDebug("Reached timeout to read (" + this.data.maxTime + ").");
            setOutputDone();
            return false;
        }

        if (checkFeedback(getLinesRead())) {
            logBasic("KafkaConsumer.Log.LineNumber - Lines read " + getLinesRead());
        }

        return true;
    }

}
