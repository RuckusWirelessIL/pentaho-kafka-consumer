package com.ruckuswireless.pentaho.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Kafka Consumer step processor
 * 
 * @author Michael Spector
 */
public class KafkaConsumerStep extends BaseStep implements StepInterface {

	public KafkaConsumerStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		super.init(smi, sdi);

		KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
		KafkaConsumerData data = (KafkaConsumerData) sdi;

		Properties properties = meta.getKafkaProperties();
		Properties substProperties = new Properties();
		for (Entry<Object, Object> e : properties.entrySet()) {
			substProperties.put(e.getKey(), environmentSubstitute(e.getValue().toString()));
		}
		ConsumerConfig consumerConfig = new ConsumerConfig(substProperties);

		logBasic(Messages.getString("KafkaConsumerStep.CreateKafkaConsumer.Message", consumerConfig.zkConnect()));
		data.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		String topic = environmentSubstitute(meta.getTopic());
		topicCountMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> streamsMap = data.consumer.createMessageStreams(topicCountMap);
		data.streamIterator = streamsMap.get(topic).get(0).iterator();

		return true;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		KafkaConsumerData data = (KafkaConsumerData) sdi;
		if (data.consumer != null) {
			data.consumer.shutdown();
		}
		super.dispose(smi, sdi);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		Object[] r = getRow();
		if (r == null) {
			/* If we have no input rows, make sure we at least run once to produce output rows.
			   This allows us to consume without requiring an input step. */
			if (!first) {
				setOutputDone();
				return false;
			}
			r = new Object[0];
		} else {
			incrementLinesRead();
		}

		KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
		final KafkaConsumerData data = (KafkaConsumerData) sdi;

		if (first) {
			first = false;
			data.inputRowMeta = getInputRowMeta();
			// No input rows means we just dummy data
			if (data.inputRowMeta == null) {
				data.outputRowMeta = new RowMeta();
				data.inputRowMeta = new RowMeta();
			} else {
				data.outputRowMeta = getInputRowMeta().clone();
			}
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
		}

		try {
			long timeout = meta.getTimeout();
			final Object[][] rClosure = new Object[][] { r };

			logDebug("Starting message consumption with overall timeout of " + timeout + "ms");

			KafkaConsumerCallable kafkaConsumer = new KafkaConsumerCallable(meta, data, this) {
				protected void messageReceived(byte[] message) throws KettleException {
					rClosure[0] = RowDataUtil.addRowData(rClosure[0], data.inputRowMeta.size(),
							new Object[] { message });
					putRow(data.outputRowMeta, rClosure[0]);

					if (isRowLevel()) {
						logRowlevel(Messages.getString("KafkaConsumerStep.Log.OutputRow",
								Long.toString(getLinesWritten()), data.outputRowMeta.getString(rClosure[0])));
					}
				}
			};
			if (timeout > 0) {
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					Future<?> future = executor.submit(kafkaConsumer);
					try {
						future.get(timeout, TimeUnit.MILLISECONDS);
					} catch (TimeoutException e) {
					} catch (Exception e) {
						throw new KettleException(e);
					}
				} finally {
					executor.shutdown();
				}
			} else {
				kafkaConsumer.call();
			}
		} catch (KettleException e) {
			if (!getStepMeta().isDoingErrorHandling()) {
				logError(Messages.getString("KafkaConsumerStep.ErrorInStepRunning", e.getMessage()));
				setErrors(1);
				stopAll();
				setOutputDone();
				return false;
			}
			putError(getInputRowMeta(), r, 1, e.toString(), null, getStepname());
		}
		return true;
	}

	public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		KafkaConsumerData data = (KafkaConsumerData) sdi;
		data.consumer.shutdown();
		data.canceled = true;

		super.stopRunning(smi, sdi);
	}
}
