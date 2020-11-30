package org.pentaho.di.trans.kafka.consumer;

import org.junit.Assert;
import org.junit.Test;

public class KafkaConsumerDataTest
{
    @Test
    public void testDefaults()
    {
        KafkaConsumerData data = new KafkaConsumerData();
        Assert.assertNull(data.outputRowMeta);
    }
}
