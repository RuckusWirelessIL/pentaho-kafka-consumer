package org.pentaho.di.trans.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ZookeeperConsumerConnector;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.mockito.Mockito.*;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({Consumer.class})
public class KafkaConsumerTest {

    Map<String, List<KafkaStream<byte[], byte[]>>> streamsMap;
    @Mock
    KafkaStream<byte[], byte[]> kafkaStream;
    @Mock
    ZookeeperConsumerConnector zookeeperConsumerConnector;
    private KafkaConsumer step;
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

        stepMeta = new StepMeta("KafkaConsumer", meta);
        transMeta = new TransMeta();
        transMeta.addStep(stepMeta);
        trans = new Trans(transMeta);

        PowerMockito.mockStatic(Consumer.class);

        streamsMap = new HashMap<String, List<KafkaStream<byte[], byte[]>>>();
        List<KafkaStream<byte[], byte[]>> stream = new ArrayList<KafkaStream<byte[], byte[]>>();
        stream.add(kafkaStream);

        streamsMap.put(meta.getTopic(), stream);
        when(Consumer.createJavaConsumerConnector(any(ConsumerConfig.class))).thenReturn(zookeeperConsumerConnector);
        when(zookeeperConsumerConnector.createMessageStreams(anyMapOf(String.class, Integer.class))).thenReturn(streamsMap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void stepInitConfigIssue() throws Exception {
        step = new KafkaConsumer(stepMeta, data, 1, transMeta, trans);
        meta.setKafkaProperties(new Properties());
        step.init(meta, data);
    }

    private Properties getDefaultKafkaProperties() {
        Properties p = new Properties();
        p.put("zookeeper.connect", "");
        p.put("group.id", "");

        return p;
    }
}
