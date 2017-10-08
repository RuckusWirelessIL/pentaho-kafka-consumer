package org.pentaho.di.trans.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ZookeeperConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransTestFactory;
import org.pentaho.di.trans.step.StepMeta;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({Consumer.class})
public class KafkaConsumerTest {

    private static final String STEP_NAME = "Kafka Step";
    private static final String STEP_LIMIT = "10000";

    @Mock
    private HashMap<String, List<KafkaStream<byte[], byte[]>>> streamsMap;
    @Mock
    private KafkaStream<byte[], byte[]> kafkaStream;
    @Mock
    private ZookeeperConsumerConnector zookeeperConsumerConnector;
    @Mock
    private ConsumerIterator<byte[], byte[]> streamIterator;
    @Mock
    private ArrayList<KafkaStream<byte[], byte[]>> stream;

    private StepMeta stepMeta;
    private KafkaConsumerMeta meta;
    private KafkaConsumerData data;
    private TransMeta transMeta;
    private Trans trans;

    @BeforeClass
    public static void setUpBeforeClass() throws KettleException {
        KettleEnvironment.init(false);
    }

    @Before
    public void setUp() {
        data = new KafkaConsumerData();
        meta = new KafkaConsumerMeta();
        meta.setKafkaProperties(getDefaultKafkaProperties());
        meta.setLimit(STEP_LIMIT);

        stepMeta = new StepMeta("KafkaConsumer", meta);
        transMeta = new TransMeta();
        transMeta.addStep(stepMeta);
        trans = new Trans(transMeta);

        PowerMockito.mockStatic(Consumer.class);

        when(Consumer.createJavaConsumerConnector(any(ConsumerConfig.class))).thenReturn(zookeeperConsumerConnector);
        when(zookeeperConsumerConnector.createMessageStreams(anyMapOf(String.class, Integer.class))).thenReturn(streamsMap);
        when(streamsMap.get(anyObject())).thenReturn(stream);
        when(stream.get(anyInt())).thenReturn(kafkaStream);
        when(kafkaStream.iterator()).thenReturn(streamIterator);
        when(streamIterator.next()).thenReturn(generateKafkaMessage());
    }

    @Test(expected = IllegalArgumentException.class)
    public void stepInitConfigIssue() throws Exception {
        KafkaConsumer step = new KafkaConsumer(stepMeta, data, 1, transMeta, trans);
        meta.setKafkaProperties(new Properties());

        step.init(meta, data);
    }

    @Test(expected = KettleException.class)
    public void illegalTimeout() throws KettleException {
        meta.setTimeout("aaa");
        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);

        TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, new ArrayList<RowMetaAndData>());

        fail("Invalid timeout value should lead to exception");
    }

    @Test(expected = KettleException.class)
    public void invalidLimit() throws KettleException {
        meta.setLimit("aaa");
        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);

        TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, new ArrayList<RowMetaAndData>());

        fail("Invalid limit value should lead to exception");
    }

    @Test
    public void withStopOnEmptyTopic() throws KettleException {

        meta.setStopOnEmptyTopic(true);
        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);

        TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, new ArrayList<RowMetaAndData>());

        PowerMockito.verifyStatic();
        ArgumentCaptor<ConsumerConfig> consumerConfig = ArgumentCaptor.forClass(ConsumerConfig.class);
        Consumer.createJavaConsumerConnector(consumerConfig.capture());

        assertEquals(1000, consumerConfig.getValue().consumerTimeoutMs());
    }

    // If the step does not receive any rows, the transformation should still run successfully
    @Test
    public void testNoInput() throws KettleException {
        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);

        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, new ArrayList<RowMetaAndData>());

        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // If the step receives rows without any fields, there should be a two output fields (key + value) on each row
    @Test
    public void testInputNoFields() throws KettleException {
        meta.setKeyField("aKeyField");
        meta.setField("aField");

        when(streamIterator.hasNext()).thenReturn(true);

        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);

        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, generateInputData(2, false));

        assertNotNull(result);
        assertEquals(Integer.parseInt(STEP_LIMIT), result.size());
        for (int i = 0; i < Integer.parseInt(STEP_LIMIT); i++) {
            assertEquals(2, result.get(i).size());
            assertEquals("aMessage", result.get(i).getString(0, "default value"));
        }
    }

    // If the step receives rows without any fields, there should be a two output fields (key + value) on each row
    @Test
    public void testInputFields() throws KettleException {
        meta.setKeyField("aKeyField");
        meta.setField("aField");

        when(streamIterator.hasNext()).thenReturn(true);

        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);

        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, generateInputData(3, true));

        assertNotNull(result);
        assertEquals(Integer.parseInt(STEP_LIMIT), result.size());
        for (int i = 0; i < Integer.parseInt(STEP_LIMIT); i++) {
            assertEquals(3, result.get(i).size());
            assertEquals("aMessage", result.get(i).getString(1, "default value"));
        }
    }

    private static Properties getDefaultKafkaProperties() {
        Properties p = new Properties();
        p.put("zookeeper.connect", "");
        p.put("group.id", "");

        return p;
    }

    /**
     * @param rowCount  The number of rows that should be returned
     * @param hasFields Whether a "UUID" field should be added to each row
     * @return A RowMetaAndData object that can be used for input data in a test transformation
     */
    private static List<RowMetaAndData> generateInputData(int rowCount, boolean hasFields) {
        List<RowMetaAndData> retval = new ArrayList<RowMetaAndData>();
        RowMetaInterface rowMeta = new RowMeta();
        if (hasFields) {
            rowMeta.addValueMeta(new ValueMetaString("UUID"));
        }

        for (int i = 0; i < rowCount; i++) {
            Object[] data = new Object[0];
            if (hasFields) {
                data = new Object[]{UUID.randomUUID().toString()};
            }
            retval.add(new RowMetaAndData(rowMeta, data));
        }
        return retval;
    }

    private static MessageAndMetadata<byte[], byte[]> generateKafkaMessage() {
        byte[] message = "aMessage".getBytes();

        return new MessageAndMetadata<byte[], byte[]>("topic", 0, new Message(message),
                0, new DefaultDecoder(null), new DefaultDecoder(null));
    }

}
