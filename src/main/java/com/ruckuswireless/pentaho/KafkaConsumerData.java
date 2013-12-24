package com.ruckuswireless.pentaho;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class KafkaConsumerData extends BaseStepData implements StepDataInterface {

	ConsumerConnector consumer;
	ConsumerIterator<byte[], byte[]> streamIterator;
	RowMetaInterface outputRowMeta;
	boolean canceled;
}
