package org.pentaho.di.trans.kafka.consumer;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;

public class KafkaConsumerMetaTest {
    @BeforeClass
    public static void setUpBeforeClass() throws KettleException
    {
        KettleEnvironment.init( false );
    }

    @Test
    public void testGetStepData() {
        KafkaConsumerMeta m = new KafkaConsumerMeta();
        Assert.assertEquals( KafkaConsumerData.class, m.getStepData().getClass() );
    }
}
