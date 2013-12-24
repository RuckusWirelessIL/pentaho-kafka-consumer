package com.ruckuswireless.pentaho;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.KafkaStream;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class KafkaConsumerStep extends BaseStep implements StepInterface {

	public KafkaConsumerStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		super.init(smi, sdi);

		KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
		KafkaConsumerData data = (KafkaConsumerData) sdi;

		data.consumer = Consumer.createJavaConsumerConnector(meta.createConsumerConfig());
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(meta.getTopic(), 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> streamsMap = data.consumer.createMessageStreams(topicCountMap);
		data.streamIterator = streamsMap.get(meta.getTopic()).get(0).iterator();

		return true;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		KafkaConsumerData data = (KafkaConsumerData) sdi;
		data.consumer.shutdown();

		super.dispose(smi, sdi);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		Object[] r = getRow();
		if (r == null) {
			setOutputDone();
			return false;
		}

		KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
		KafkaConsumerData data = (KafkaConsumerData) sdi;

		if (first) {
			first = false;
			data.outputRowMeta = getInputRowMeta().clone();
			RowMetaInterface rowMeta = new RowMeta();
			rowMeta.addValueMeta(new ValueMeta(meta.getField(), ValueMetaInterface.TYPE_BINARY));
			data.outputRowMeta.mergeRowMeta(rowMeta);
		}

		try {
			while (data.streamIterator.hasNext() && !data.canceled) {
				r = RowDataUtil.addRowData(r, getInputRowMeta().size(), new Object[] { data.streamIterator.next()
						.message() });
				putRow(data.outputRowMeta, r);

				if (isRowLevel()) {
					logRowlevel(Messages.getString("KafkaConsumerStep.Log.OutputRow", Long.toString(getLinesWritten()),
							getInputRowMeta().getString(r)));
				}
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
		data.canceled = true;

		super.stopRunning(smi, sdi);
	}
}
