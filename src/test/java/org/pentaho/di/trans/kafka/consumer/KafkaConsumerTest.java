package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaConsumer.class})
public class KafkaConsumerTest
{
    private static final String STEP_NAME = "Kafka Step";
    private static final String STEP_TOPIC = "Test";
    private static final String STEP_LIMIT = "10";
    private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer;
    private StepMeta stepMeta;
    private KafkaConsumerMeta meta;
    private KafkaConsumerData data;
    private TransMeta transMeta;
    private Trans trans;


    @BeforeClass
    public static void setUpBeforeClass() throws KettleException
    {
        KettleEnvironment.init(false);
    }

    @Before
    public void setUp() throws Exception
    {
        data = new KafkaConsumerData();
        meta = new KafkaConsumerMeta();

        // Default values.
        meta.setTopic(STEP_TOPIC);
        meta.setTimeout("0");
        meta.setLimit("0");
        meta.getKafkaProperties().setProperty("bootstrap.servers", meta.defaultProp("bootstrap.servers"));
        meta.getKafkaProperties().setProperty("key.deserializer", meta.defaultProp("key.deserializer"));
        meta.getKafkaProperties().setProperty("value.deserializer", meta.defaultProp("key.deserializer"));

        stepMeta = new StepMeta("KafkaConsumer", meta);
        transMeta = new TransMeta();
        transMeta.addStep(stepMeta);
        trans = new Trans(transMeta);

        // Mock Kafka consumer.
        this.consumer = PowerMockito.mock(org.apache.kafka.clients.consumer.KafkaConsumer.class);
        PowerMockito.whenNew(org.apache.kafka.clients.consumer.KafkaConsumer.class)
                    .withParameterTypes(Properties.class)
                    .withArguments(any(Properties.class))
                    .thenReturn(this.consumer);
    }

    @Test(expected = KettleException.class)
    public void illegalTimeout() throws KettleException
    {
        meta.setTimeout("aaa");
        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);
        PowerMockito.when(this.consumer.poll(anyLong())).thenReturn(ConsumerRecords.empty());

        TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, new ArrayList<RowMetaAndData>());

        fail("Invalid timeout value should lead to exception");
    }

    @Test(expected = KettleException.class)
    public void invalidLimit() throws KettleException
    {
        meta.setLimit("aaa");
        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);
        PowerMockito.when(this.consumer.poll(anyLong())).thenReturn(ConsumerRecords.empty());

        TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, new ArrayList<RowMetaAndData>());

        fail("Invalid limit value should lead to exception");
    }

    // If the step does not receive any rows, the transformation should run successfully
    @Test
    public void testNoInput() throws KettleException
    {
        meta.setTimeout("1000");

        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);
        PowerMockito.when(this.consumer.poll(anyLong())).thenReturn(ConsumerRecords.empty());

        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, new ArrayList<RowMetaAndData>());

        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // If the step receives rows without any fields, there should be a two output fields (key + value) on each row
    @Test
    public void testInputNoFields() throws KettleException
    {
        meta.setKeyField("aKeyField");
        meta.setField("aField");
        meta.setLimit(STEP_LIMIT);

        TransMeta tm = TransTestFactory.generateTestTransformation(new Variables(), meta, STEP_NAME);
        PowerMockito.when(this.consumer.poll(anyLong())).thenReturn(this.generateRecords());

        List<RowMetaAndData> result = TransTestFactory.executeTestTransformation(tm, TransTestFactory.INJECTOR_STEPNAME,
                STEP_NAME, TransTestFactory.DUMMY_STEPNAME, generateInputData(2, false));

        assertNotNull(result);
        assertEquals(Integer.parseInt(STEP_LIMIT), result.size());
        for (int i = 0; i < Integer.parseInt(STEP_LIMIT); i++) {
            assertEquals(2, result.get(i).size());
            assertEquals("value", result.get(i).getString(1, "null"));
        }
    }


    /**
     * @param rowCount  The number of rows that should be returned
     * @param hasFields Whether a "UUID" field should be added to each row
     * @return A RowMetaAndData object that can be used for input data in a test transformation
     */
    private static List<RowMetaAndData> generateInputData(int rowCount, boolean hasFields)
    {
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

    private ConsumerRecords<byte[], byte[]> generateRecords()
    {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsHM = new HashMap<>();
        ConsumerRecord record = new ConsumerRecord(STEP_TOPIC, 0, 0, "key", "value");
        TopicPartition tp = new TopicPartition(STEP_TOPIC, 0);
        List<ConsumerRecord<byte[], byte[]>> list = new ArrayList<>();
        list.add(record);
        recordsHM.put(tp, list);
        return new ConsumerRecords<>(recordsHM);
    }

}