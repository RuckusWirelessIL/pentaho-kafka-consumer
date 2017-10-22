package org.pentaho.di.trans.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Consumer step definitions and serializer to/from XML and to/from Kettle
 * repository.
 *
 * @author Michael Spector
 */
@Step(
        id = "KafkaConsumer",
        image = "org/pentaho/di/trans/kafka/consumer/resources/kafka_consumer.png",
        i18nPackageName = "org.pentaho.di.trans.kafka.consumer",
        name = "KafkaConsumerDialog.Shell.Title",
        description = "KafkaConsumerDialog.Shell.Tooltip",
        documentationUrl = "KafkaConsumerDialog.Shell.DocumentationURL",
        casesUrl = "KafkaConsumerDialog.Shell.CasesURL",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Input")
public class KafkaConsumerMeta extends BaseStepMeta implements StepMetaInterface {

    @SuppressWarnings("WeakerAccess")
    protected static final String[] KAFKA_PROPERTIES_NAMES = new String[]{"zookeeper.connect", "group.id", "consumer.id",
            "socket.timeout.ms", "socket.receive.buffer.bytes", "fetch.message.max.bytes", "auto.commit.interval.ms",
            "queued.max.message.chunks", "rebalance.max.retries", "fetch.min.bytes", "fetch.wait.max.ms",
            "rebalance.backoff.ms", "refresh.leader.backoff.ms", "auto.commit.enable", "auto.offset.reset",
            "consumer.timeout.ms", "client.id", "zookeeper.session.timeout.ms", "zookeeper.connection.timeout.ms",
            "zookeeper.sync.time.ms"};

    @SuppressWarnings("WeakerAccess")
    protected static final Map<String, String> KAFKA_PROPERTIES_DEFAULTS = new HashMap<String, String>();

    private static final String ATTR_TOPIC = "TOPIC";
    private static final String ATTR_FIELD = "FIELD";
    private static final String ATTR_KEY_FIELD = "KEY_FIELD";
    private static final String ATTR_LIMIT = "LIMIT";
    private static final String ATTR_TIMEOUT = "TIMEOUT";
    private static final String ATTR_STOP_ON_EMPTY_TOPIC = "STOP_ON_EMPTY_TOPIC";
    private static final String ATTR_KAFKA = "KAFKA";

    static {
        KAFKA_PROPERTIES_DEFAULTS.put("zookeeper.connect", "localhost:2181");
        KAFKA_PROPERTIES_DEFAULTS.put("group.id", "group");
    }

    private Properties kafkaProperties = new Properties();
    private String topic;
    private String field;
    private String keyField;
    private String limit;
    private String timeout;
    private boolean stopOnEmptyTopic;

    public static String[] getKafkaPropertiesNames() {
        return KAFKA_PROPERTIES_NAMES;
    }

    public static Map<String, String> getKafkaPropertiesDefaults() {
        return KAFKA_PROPERTIES_DEFAULTS;
    }

    public KafkaConsumerMeta() {
        super();
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    @SuppressWarnings("unused")
    public Map getKafkaPropertiesMap() {
        return getKafkaProperties();
    }

    public void setKafkaProperties(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @SuppressWarnings("unused")
    public void setKafkaPropertiesMap(Map<String, String> propertiesMap) {
        Properties props = new Properties();
        props.putAll(propertiesMap);
        setKafkaProperties(props);
    }

    /**
     * @return Kafka topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @param topic Kafka topic name
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * @return Target field name in Kettle stream
     */
    public String getField() {
        return field;
    }

    /**
     * @param field Target field name in Kettle stream
     */
    public void setField(String field) {
        this.field = field;
    }

    /**
     * @return Target key field name in Kettle stream
     */
    public String getKeyField() {
        return keyField;
    }

    /**
     * @param keyField Target key field name in Kettle stream
     */
    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    /**
     * @return Limit number of entries to read from Kafka queue
     */
    public String getLimit() {
        return limit;
    }

    /**
     * @param limit Limit number of entries to read from Kafka queue
     */
    public void setLimit(String limit) {
        this.limit = limit;
    }

    /**
     * @return Time limit for reading entries from Kafka queue (in ms)
     */
    public String getTimeout() {
        return timeout;
    }

    /**
     * @param timeout Time limit for reading entries from Kafka queue (in ms)
     */
    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    /**
     * @return 'true' if the consumer should stop when no more messages are
     * available
     */
    public boolean isStopOnEmptyTopic() {
        return stopOnEmptyTopic;
    }

    /**
     * @param stopOnEmptyTopic If 'true', stop the consumer when no more messages are
     *                         available on the topic
     */
    public void setStopOnEmptyTopic(boolean stopOnEmptyTopic) {
        this.stopOnEmptyTopic = stopOnEmptyTopic;
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore) {

        if (topic == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    Messages.getString("KafkaConsumerMeta.Check.InvalidTopic"), stepMeta));
        }
        if (field == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    Messages.getString("KafkaConsumerMeta.Check.InvalidField"), stepMeta));
        }
        if (keyField == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    Messages.getString("KafkaConsumerMeta.Check.InvalidKeyField"), stepMeta));
        }
        try {
            new ConsumerConfig(kafkaProperties);
        } catch (IllegalArgumentException e) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, e.getMessage(), stepMeta));
        }
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                 Trans trans) {
        return new KafkaConsumer(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    public StepDataInterface getStepData() {
        return new KafkaConsumerData();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
            throws KettleXMLException {

        try {
            topic = XMLHandler.getTagValue(stepnode, ATTR_TOPIC);
            field = XMLHandler.getTagValue(stepnode, ATTR_FIELD);
            keyField = XMLHandler.getTagValue(stepnode, ATTR_KEY_FIELD);
            limit = XMLHandler.getTagValue(stepnode, ATTR_LIMIT);
            timeout = XMLHandler.getTagValue(stepnode, ATTR_TIMEOUT);
            // This tag only exists if the value is "true", so we can directly
            // populate the field
            stopOnEmptyTopic = XMLHandler.getTagValue(stepnode, ATTR_STOP_ON_EMPTY_TOPIC) != null;
            Node kafkaNode = XMLHandler.getSubNode(stepnode, ATTR_KAFKA);
            String[] kafkaElements = XMLHandler.getNodeElements(kafkaNode);
            if (kafkaElements != null) {
                for (String propName : kafkaElements) {
                    String value = XMLHandler.getTagValue(kafkaNode, propName);
                    if (value != null) {
                        kafkaProperties.put(propName, value);
                    }
                }
            }
        } catch (Exception e) {
            throw new KettleXMLException(Messages.getString("KafkaConsumerMeta.Exception.loadXml"), e);
        }
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder retval = new StringBuilder();
        if (topic != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_TOPIC, topic));
        }
        if (field != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_FIELD, field));
        }
        if (keyField != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_KEY_FIELD, keyField));
        }
        if (limit != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_LIMIT, limit));
        }
        if (timeout != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_TIMEOUT, timeout));
        }
        if (stopOnEmptyTopic) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_STOP_ON_EMPTY_TOPIC, "true"));
        }
        retval.append("    ").append(XMLHandler.openTag(ATTR_KAFKA)).append(Const.CR);
        for (String name : kafkaProperties.stringPropertyNames()) {
            String value = kafkaProperties.getProperty(name);
            if (value != null) {
                retval.append("      ").append(XMLHandler.addTagValue(name, value));
            }
        }
        retval.append("    ").append(XMLHandler.closeTag(ATTR_KAFKA)).append(Const.CR);
        return retval.toString();
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases)
            throws KettleException {
        try {
            topic = rep.getStepAttributeString(stepId, ATTR_TOPIC);
            field = rep.getStepAttributeString(stepId, ATTR_FIELD);
            keyField = rep.getStepAttributeString(stepId, ATTR_KEY_FIELD);
            limit = rep.getStepAttributeString(stepId, ATTR_LIMIT);
            timeout = rep.getStepAttributeString(stepId, ATTR_TIMEOUT);
            stopOnEmptyTopic = rep.getStepAttributeBoolean(stepId, ATTR_STOP_ON_EMPTY_TOPIC);
            String kafkaPropsXML = rep.getStepAttributeString(stepId, ATTR_KAFKA);
            if (kafkaPropsXML != null) {
                kafkaProperties.loadFromXML(new ByteArrayInputStream(kafkaPropsXML.getBytes()));
            }
            // Support old versions:
            for (String name : KAFKA_PROPERTIES_NAMES) {
                String value = rep.getStepAttributeString(stepId, name);
                if (value != null) {
                    kafkaProperties.put(name, value);
                }
            }
        } catch (Exception e) {
            throw new KettleException("KafkaConsumerMeta.Exception.loadRep", e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId) throws KettleException {
        try {
            if (topic != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_TOPIC, topic);
            }
            if (field != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_FIELD, field);
            }
            if (keyField != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_KEY_FIELD, keyField);
            }
            if (limit != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_LIMIT, limit);
            }
            if (timeout != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_TIMEOUT, timeout);
            }
            rep.saveStepAttribute(transformationId, stepId, ATTR_STOP_ON_EMPTY_TOPIC, stopOnEmptyTopic);

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            kafkaProperties.storeToXML(buf, null);
            rep.saveStepAttribute(transformationId, stepId, ATTR_KAFKA, buf.toString());
        } catch (Exception e) {
            throw new KettleException("KafkaConsumerMeta.Exception.saveRep", e);
        }
    }

    /**
     * Set default values to the transformation
     */
    public void setDefault() {
        setTopic("");
    }

    public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

        try {
            ValueMetaInterface fieldValueMeta = ValueMetaFactory.createValueMeta(getField(), ValueMetaInterface.TYPE_BINARY);
            fieldValueMeta.setOrigin(origin);
            rowMeta.addValueMeta(fieldValueMeta);

            ValueMetaInterface keyFieldValueMeta = ValueMetaFactory.createValueMeta(getKeyField(), ValueMetaInterface.TYPE_BINARY);
            keyFieldValueMeta.setOrigin(origin);
            rowMeta.addValueMeta(keyFieldValueMeta);

        } catch (KettlePluginException e) {
            throw new KettleStepException("KafkaConsumerMeta.Exception.getFields", e);
        }

    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

}
