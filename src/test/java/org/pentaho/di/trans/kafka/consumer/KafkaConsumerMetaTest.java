package org.pentaho.di.trans.kafka.consumer;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.MemoryRepository;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.MapLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.Assert.*;

public class KafkaConsumerMetaTest
{
    @BeforeClass
    public static void setUpBeforeClass() throws KettleException
    {
        KettleEnvironment.init(false);
    }

    @Test
    public void testGetStepData()
    {
        KafkaConsumerMeta m = new KafkaConsumerMeta();
        assertEquals(KafkaConsumerData.class, m.getStepData().getClass());
    }

    @Test
    public void testStepAnnotations()
    {
        // PDI Plugin Annotation-based Classloader checks
        Step stepAnnotation = KafkaConsumerMeta.class.getAnnotation(Step.class);
        assertNotNull(stepAnnotation);
        assertFalse(Utils.isEmpty(stepAnnotation.id()));
        assertFalse(Utils.isEmpty(stepAnnotation.name()));
        assertFalse(Utils.isEmpty(stepAnnotation.description()));
        assertFalse(Utils.isEmpty(stepAnnotation.image()));
        assertFalse(Utils.isEmpty(stepAnnotation.categoryDescription()));
        assertFalse(Utils.isEmpty(stepAnnotation.i18nPackageName()));
        assertFalse(Utils.isEmpty(stepAnnotation.documentationUrl()));
        assertFalse(Utils.isEmpty(stepAnnotation.casesUrl()));
        assertEquals(KafkaConsumerMeta.class.getPackage().getName(), stepAnnotation.i18nPackageName());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.name());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.description());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.documentationUrl());
        hasi18nValue(stepAnnotation.i18nPackageName(), stepAnnotation.casesUrl());
    }

    @Test
    public void testDefaults() throws KettleStepException
    {
        KafkaConsumerMeta m = new KafkaConsumerMeta();
        m.setDefault();

        RowMetaInterface rowMeta = new RowMeta();
        m.getFields(rowMeta, "kafka_consumer", null, null, null, null, null);

        // expect two fields to be added to the row stream
        assertEquals(2, rowMeta.size());

        // those fields must strings and named as configured
        assertEquals(ValueMetaInterface.TYPE_STRING, rowMeta.getValueMeta(0).getType());
        assertEquals(ValueMetaInterface.TYPE_STRING, rowMeta.getValueMeta(1).getType());
        assertEquals(ValueMetaInterface.STORAGE_TYPE_NORMAL, rowMeta.getValueMeta(0).getStorageType());
        assertEquals(ValueMetaInterface.STORAGE_TYPE_NORMAL, rowMeta.getValueMeta(1).getStorageType());
        // TODO check naming
        //assertEquals( rowMeta.getFieldNames()[0], m.getOutputField() );
    }

    @Test
    public void testLoadSave() throws KettleException
    {
        List<String> attributes = Arrays.asList("topic", "field", "keyField", "limit", "timeout", "kafka", "stopOnEmptyTopic");

        Map<String, String> getterMap = new HashMap<>();
        getterMap.put("topic", "getTopic");
        getterMap.put("field", "getField");
        getterMap.put("keyField", "getKeyField");
        getterMap.put("limit", "getLimit");
        getterMap.put("timeout", "getTimeout");
        getterMap.put("kafka", "getKafkaPropertiesMap");
        getterMap.put("stopOnEmptyTopic", "isStopOnEmptyTopic");

        Map<String, String> setterMap = new HashMap<>();
        setterMap.put("topic", "setTopic");
        setterMap.put("field", "setField");
        setterMap.put("keyField", "setKeyField");
        setterMap.put("limit", "setLimit");
        setterMap.put("timeout", "setTimeout");
        setterMap.put("kafka", "setKafkaPropertiesMap");
        setterMap.put("stopOnEmptyTopic", "setStopOnEmptyTopic");

        Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();
        Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap = new HashMap<>();
        fieldLoadSaveValidatorAttributeMap.put("kafka", new MapLoadSaveValidator<>(
                new KeyStringLoadSaveValidator(), new StringLoadSaveValidator()));

        LoadSaveTester tester = new LoadSaveTester(KafkaConsumerMeta.class, attributes, getterMap, setterMap, fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap);

        tester.testSerialization();
    }

    @Test
    public void testChecksEmpty()
    {
        KafkaConsumerMeta m = new KafkaConsumerMeta();

        // Test missing Topic name
        List<CheckResultInterface> checkResults = new ArrayList<>();
        m.check(checkResults, new TransMeta(), new StepMeta(), null, null, null, null, new Variables(), new MemoryRepository(), null);
        assertFalse(checkResults.isEmpty());
        boolean foundMatch = false;
        for (CheckResultInterface result : checkResults) {
            if (result.getType() == CheckResultInterface.TYPE_RESULT_ERROR
                    && result.getText().equals(BaseMessages.getString(KafkaConsumerMeta.class, "KafkaConsumerMeta.Check.InvalidTopic"))) {
                foundMatch = true;
            }
        }
        assertTrue("The step checks should fail if input topic is not given", foundMatch);

        // Test missing field name
        foundMatch = false;
        for (CheckResultInterface result : checkResults) {
            if (result.getType() == CheckResultInterface.TYPE_RESULT_ERROR
                    && result.getText().equals(BaseMessages.getString(KafkaConsumerMeta.class, "KafkaConsumerMeta.Check.InvalidField"))) {
                foundMatch = true;
            }
        }
        assertTrue("The step checks should fail if field is not given", foundMatch);

        // Test missing Key field name
        foundMatch = false;
        for (CheckResultInterface result : checkResults) {
            if (result.getType() == CheckResultInterface.TYPE_RESULT_ERROR
                    && result.getText().equals(BaseMessages.getString(KafkaConsumerMeta.class, "KafkaConsumerMeta.Check.InvalidKeyField"))) {
                foundMatch = true;
            }
        }
        assertTrue("The step checks should fail if key is not given", foundMatch);
    }

    @Test
    public void testChecksNotEmpty()
    {
        KafkaConsumerMeta m = new KafkaConsumerMeta();
        m.setTopic(UUID.randomUUID().toString());
        m.setField(UUID.randomUUID().toString());
        m.setKeyField(UUID.randomUUID().toString());

        // Test present Topic name
        List<CheckResultInterface> checkResults = new ArrayList<>();
        m.check(checkResults, new TransMeta(), new StepMeta(), null, null, null, null, new Variables(), new MemoryRepository(), null);
        assertFalse(checkResults.isEmpty());
        boolean foundMatch = false;
        for (CheckResultInterface result : checkResults) {
            if (result.getType() == CheckResultInterface.TYPE_RESULT_ERROR
                    && result.getText().equals(BaseMessages.getString(KafkaConsumerMeta.class, "KafkaConsumerMeta.Check.InvalidTopic"))) {
                foundMatch = true;
            }
        }
        assertFalse("The step checks should not fail if input topic is given", foundMatch);

        // Test missing field name
        foundMatch = false;
        for (CheckResultInterface result : checkResults) {
            if (result.getType() == CheckResultInterface.TYPE_RESULT_ERROR
                    && result.getText().equals(BaseMessages.getString(KafkaConsumerMeta.class, "KafkaConsumerMeta.Check.InvalidField"))) {
                foundMatch = true;
            }
        }
        assertFalse("The step checks should not fail if field is given", foundMatch);

        // Test missing Key field name
        foundMatch = false;
        for (CheckResultInterface result : checkResults) {
            if (result.getType() == CheckResultInterface.TYPE_RESULT_ERROR
                    && result.getText().equals(BaseMessages.getString(KafkaConsumerMeta.class, "KafkaConsumerMeta.Check.InvalidKeyField"))) {
                foundMatch = true;
            }
        }
        assertFalse("The step checks should not fail if key is given", foundMatch);

    }

    /**
     * Private class to generate alphabetic xml tags
     */
    private class KeyStringLoadSaveValidator extends StringLoadSaveValidator
    {
        @Override
        public String getTestObject() {
            return "k" + UUID.randomUUID().toString();
        }
    }

    private void hasi18nValue(String i18nPackageName, String messageId)
    {
        String fakeId = UUID.randomUUID().toString();
        String fakeLocalized = BaseMessages.getString(i18nPackageName, fakeId);
        assertEquals("The way to identify a missing localization key has changed", "!" + fakeId + "!", fakeLocalized);

        // Real Test
        String localized = BaseMessages.getString(i18nPackageName, messageId);
        assertFalse(Utils.isEmpty(localized));
        assertNotEquals("!" + messageId + "!", localized);
    }

}
