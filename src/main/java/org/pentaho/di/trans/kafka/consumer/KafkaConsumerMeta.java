package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;

import org.w3c.dom.Node;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Consumer step definitions and serializer to/from XML and to/from Kettle
 * repository.
 *
 * @author Michael Spector
 * @author Miguel Ángel García
 */
@Step(  id = "KafkaConsumer",
        image = "org/pentaho/di/trans/kafka/consumer/resources/kafka_consumer.png",
        i18nPackageName = "org.pentaho.di.trans.kafka.consumer",
        name = "KafkaConsumerDialog.Shell.Title",
        description = "KafkaConsumerDialog.Shell.Tooltip",
        documentationUrl = "KafkaConsumerDialog.Shell.DocumentationURL",
        casesUrl = "KafkaConsumerDialog.Shell.CasesURL",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Input")
public class KafkaConsumerMeta extends BaseStepMeta implements StepMetaInterface
{
    @SuppressWarnings("WeakerAccess")
    protected static final String[] KAFKA_PROPERTIES_NAMES = new String[]{
            // Server props.
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
            ConsumerConfig.GROUP_ID_CONFIG, ConsumerConfig.CLIENT_ID_CONFIG,
            // Fetching props.
            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            // Security props.
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SaslConfigs.SASL_MECHANISM, SaslConfigs.SASL_JAAS_CONFIG
    };

    private Properties kafkaProperties = new Properties();

    // Kafka consumer step behaviour keys.
    private static final String ATTR_TOPIC = "TOPIC";
    private static final String ATTR_FIELD = "FIELD";
    private static final String ATTR_KEY_FIELD = "KEY_FIELD";
    private static final String ATTR_LIMIT = "LIMIT";
    private static final String ATTR_TIMEOUT = "TIMEOUT";
    private static final String ATTR_STOP_ON_EMPTY_TOPIC = "STOP_ON_EMPTY_TOPIC";
    private static final String ATTR_KAFKA = "KAFKA";
    // Kafka consumer step behaviour vars.
    private String topic;
    private String field;
    private String keyField;
    private String limit;
    private String timeout;
    private boolean stopOnEmptyTopic;


    /**
     * Default constructor.
     */
    public KafkaConsumerMeta()
    {
        super();
    }


    /**
     * Checks the step when a user presses on the check transformation button.
     */
    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore)
    {
        if (this.topic == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    Messages.getString("KafkaConsumerMeta.Check.InvalidTopic"), stepMeta));
        }
        if (this.field == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    Messages.getString("KafkaConsumerMeta.Check.InvalidField"), stepMeta));
        }
        if (this.keyField == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    Messages.getString("KafkaConsumerMeta.Check.InvalidKeyField"), stepMeta));
        }
        try {
            new org.apache.kafka.clients.consumer.KafkaConsumer<>(this.kafkaProperties);
        } catch (ConfigException e) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, e.getMessage(), stepMeta));
        }
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans)
    {
        return new KafkaConsumer(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData()
    {
        return new KafkaConsumerData();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
    {
        this.topic = XMLHandler.getTagValue(stepnode, ATTR_TOPIC);
        this.field = XMLHandler.getTagValue(stepnode, ATTR_FIELD);
        this.keyField = XMLHandler.getTagValue(stepnode, ATTR_KEY_FIELD);
        this.limit = XMLHandler.getTagValue(stepnode, ATTR_LIMIT);
        this.timeout = XMLHandler.getTagValue(stepnode, ATTR_TIMEOUT);
        // This tag only exists if the value is "true", so we can directly populate the field.
        this.stopOnEmptyTopic = XMLHandler.getTagValue(stepnode, ATTR_STOP_ON_EMPTY_TOPIC) != null;
        Node kafkaNode = XMLHandler.getSubNode(stepnode, ATTR_KAFKA);
        String[] kafkaElements = XMLHandler.getNodeElements(kafkaNode);
        if (kafkaElements != null) {
            for (String propName : kafkaElements) {
                String value = XMLHandler.getTagValue(kafkaNode, propName);
                if (value != null) {
                    this.kafkaProperties.put(propName, value);
                }
            }
        }
    }

    @Override
    public String getXML()
    {
        StringBuilder retval = new StringBuilder();
        if (this.topic != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_TOPIC, topic));
        }
        if (this.field != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_FIELD, field));
        }
        if (this.keyField != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_KEY_FIELD, keyField));
        }
        if (this.limit != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_LIMIT, limit));
        }
        if (this.timeout != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_TIMEOUT, timeout));
        }
        if (this.stopOnEmptyTopic) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_STOP_ON_EMPTY_TOPIC, "true"));
        }
        retval.append("    ").append(XMLHandler.openTag(ATTR_KAFKA)).append(Const.CR);
        for (String name : this.kafkaProperties.stringPropertyNames()) {
            String value = this.kafkaProperties.getProperty(name);
            if (value != null) {
                retval.append("      ").append(XMLHandler.addTagValue(name, value));
            }
        }
        retval.append("    ").append(XMLHandler.closeTag(ATTR_KAFKA)).append(Const.CR);
        return retval.toString();
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases) throws KettleException
    {
        try {
            this.topic = rep.getStepAttributeString(stepId, ATTR_TOPIC);
            this.field = rep.getStepAttributeString(stepId, ATTR_FIELD);
            this.keyField = rep.getStepAttributeString(stepId, ATTR_KEY_FIELD);
            this.limit = rep.getStepAttributeString(stepId, ATTR_LIMIT);
            this.timeout = rep.getStepAttributeString(stepId, ATTR_TIMEOUT);
            this.stopOnEmptyTopic = rep.getStepAttributeBoolean(stepId, ATTR_STOP_ON_EMPTY_TOPIC);
            String kafkaPropsXML = rep.getStepAttributeString(stepId, ATTR_KAFKA);
            if (kafkaPropsXML != null) {
                this.kafkaProperties.loadFromXML(new ByteArrayInputStream(kafkaPropsXML.getBytes()));
            }
        } catch (Exception e) {
            throw new KettleException("KafkaConsumerMeta.Exception.loadRep", e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId) throws KettleException
    {
        try {
            if (this.topic != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_TOPIC, this.topic);
            }
            if (this.field != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_FIELD, this.field);
            }
            if (this.keyField != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_KEY_FIELD, this.keyField);
            }
            if (this.limit != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_LIMIT, this.limit);
            }
            if (this.timeout != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_TIMEOUT, this.timeout);
            }
            rep.saveStepAttribute(transformationId, stepId, ATTR_STOP_ON_EMPTY_TOPIC, this.stopOnEmptyTopic);

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            this.kafkaProperties.storeToXML(buf, null);
            rep.saveStepAttribute(transformationId, stepId, ATTR_KAFKA, buf.toString());
        } catch (Exception e) {
            throw new KettleException("KafkaConsumerMeta.Exception.saveRep", e);
        }
    }

    /**
     * Set default values to the transformation.
     */
    @Override
    public void setDefault()
    {
        this.topic = "";
        this.keyField = "key";
        this.field = "msg";
        this.limit = "0";
        this.timeout = "0";
    }

    /**
     * Modifies the input row adding new fields to the row structure.
     */
    @Override
    public void getFields(RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException
    {
        try {
            // Add the key field to the row.
            ValueMetaInterface keyValueMeta = ValueMetaFactory.createValueMeta(this.keyField, ValueMetaInterface.TYPE_STRING);
            keyValueMeta.setOrigin(name);
            rowMeta.addValueMeta(keyValueMeta);
            // Add the message field to the row.
            ValueMetaInterface messageValueMeta = ValueMetaFactory.createValueMeta(this.field, ValueMetaInterface.TYPE_STRING);
            messageValueMeta.setOrigin(name);
            rowMeta.addValueMeta(messageValueMeta);
        } catch (KettlePluginException e) {
            throw new KettleStepException("KafkaConsumerMeta.Exception.getFields", e);
        }
    }

    /**
     * Gets the default value for the props.
     */
    public static String defaultProp(String prop)
    {
        switch (prop) {
            case ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG:
                return "localhost:2181";
            case ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG:
            case ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG:
                return "org.apache.kafka.common.serialization.StringDeserializer";
            case ConsumerConfig.AUTO_OFFSET_RESET_CONFIG:
                return "earliest";
            case ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG:
                return "true";
            case ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG:
                return "10000";
            case ConsumerConfig.MAX_POLL_RECORDS_CONFIG:
                return "1";
            case CommonClientConfigs.SECURITY_PROTOCOL_CONFIG:
                return "SASL_SSL";
            case SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG:
                return "/var/www/data/trustore.jks";
            case SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG:
                return "1234567890";
            case SaslConfigs.SASL_MECHANISM:
                return "PLAIN";
            case SaslConfigs.SASL_JAAS_CONFIG:
                return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"mr.robot\" password=\"12345\" serviceName=\"kafka\";";
            default:
                return "(default)";
        }
    }


    // Getters & Setters.

    @SuppressWarnings("unused")
    public Map getKafkaPropertiesMap()
    {
        return this.getKafkaProperties();
    }

    @SuppressWarnings("unused")
    public void setKafkaPropertiesMap(Map<String, String> propertiesMap)
    {
        Properties props = new Properties();
        props.putAll(propertiesMap);
        this.kafkaProperties = props;
    }

    public static String[] getKafkaPropertiesNames()
    {
        return KafkaConsumerMeta.KAFKA_PROPERTIES_NAMES;
    }

    public Properties getKafkaProperties()
    {
        return this.kafkaProperties;
    }

    public void setKafkaProperties(Properties kafkaProperties)
    {
        this.kafkaProperties = kafkaProperties;
    }

    public String getTopic()
    {
        return this.topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    public String getField()
    {
        return this.field;
    }

    public void setField(String field)
    {
        this.field = field;
    }

    public String getKeyField()
    {
        return this.keyField;
    }

    public void setKeyField(String keyField)
    {
        this.keyField = keyField;
    }

    public String getLimit()
    {
        return this.limit;
    }

    public void setLimit(String limit)
    {
        this.limit = limit;
    }

    public String getTimeout()
    {
        return this.timeout;
    }

    public void setTimeout(String timeout)
    {
        this.timeout = timeout;
    }

    public boolean isStopOnEmptyTopic()
    {
        return this.stopOnEmptyTopic;
    }

    public void setStopOnEmptyTopic(boolean stopOnEmptyTopic) {
        this.stopOnEmptyTopic = stopOnEmptyTopic;
    }
}
