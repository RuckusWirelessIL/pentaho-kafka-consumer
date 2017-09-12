package org.pentaho.di.trans.kafka.consumer;

import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

import java.util.UUID;

public class KafkaConsumerMetaTest {
    @BeforeClass
    public static void setUpBeforeClass() throws KettleException
    {
        KettleEnvironment.init( false );
    }

    @Test
    public void testGetStepData() {
        KafkaConsumerMeta m = new KafkaConsumerMeta();
        assertEquals(KafkaConsumerData.class, m.getStepData().getClass() );
    }

    @Test
    public void testStepAnnotations() {

        // PDI Plugin Annotation-based Classloader checks
        Step stepAnnotation = KafkaConsumerMeta.class.getAnnotation(Step.class);
        assertNotNull(stepAnnotation);
        assertFalse(Const.isEmpty(stepAnnotation.id()));
        assertFalse(Const.isEmpty(stepAnnotation.name()));
        assertFalse(Const.isEmpty(stepAnnotation.description()));
        assertFalse(Const.isEmpty(stepAnnotation.image()));
        assertFalse(Const.isEmpty(stepAnnotation.categoryDescription()));
        assertFalse(Const.isEmpty(stepAnnotation.i18nPackageName()));
        assertFalse(Const.isEmpty(stepAnnotation.documentationUrl()));
        assertFalse(Const.isEmpty(stepAnnotation.casesUrl()));
        assertEquals(KafkaConsumerMeta.class.getPackage().getName(), stepAnnotation.i18nPackageName());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.name());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.description());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.documentationUrl());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.casesUrl());
    }

    private void hasi18nValue( String i18nPackageName, String messageId ) {
        String fakeId = UUID.randomUUID().toString();
        String fakeLocalized = BaseMessages.getString( i18nPackageName, fakeId );
        assertEquals( "The way to identify a missing localization key has changed", "!" + fakeId + "!", fakeLocalized );

        // Real Test
        String localized = BaseMessages.getString( i18nPackageName, messageId );
        assertFalse( Const.isEmpty( localized ) );
        assertNotEquals( "!" + messageId + "!", localized );
    }
}
