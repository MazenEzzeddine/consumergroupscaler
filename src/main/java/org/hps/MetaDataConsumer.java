package org.hps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class MetaDataConsumer /*implements Configurable*/ {

    private Properties consumerGroupProps;
    private Properties metadataConsumerProps;
    private KafkaConsumer metadataConsumer;


    private static final Logger log = LogManager.getLogger(Scaler.class);


    /*@Override
    public void configure(Map<String, ?> configs) {

        // Construct Properties from config map
        consumerGroupProps = new Properties();
        for (final Map.Entry<String, ?> prop : configs.entrySet()) {
            consumerGroupProps.put(prop.getKey(), prop.getValue());
        }

        // group.id must be defined
        final String groupId = "testgroup2";//consumerGroupProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);


        // Create a new consumer that can be used to get lag metadata for the consumer group
        metadataConsumerProps = new Properties();
        metadataConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup2");

        metadataConsumerProps.putAll(consumerGroupProps);
        metadataConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        final String clientId = groupId + ".metadada";
        metadataConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        log.info(
                "Configured metadata consumer with values:\n"
                        + "\tgroup.id = {}\n"
                        + "\tclient.id = {}\n",
                groupId,
                clientId
        );

    }*/



    public void createDirectConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup2");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        metadataConsumer = new KafkaConsumer<String, String>(props);


        metadataConsumer.subscribe(Collections.singletonList("testtopic2"));

    }



    public void createMetaConsumer() {

        log.info("creating meta data consumer");

        metadataConsumer = new KafkaConsumer<>(metadataConsumerProps);
        log.info("created meta data consumer");


    }


    public void consumerEnforceRebalance() {

        log.info("Trying to enforce Rebalance");

        //metadataConsumer.enforceRebalance();

        metadataConsumer.enforceRebalance();
        log.info("Finalizing  enforce Rebalance");
    }
}
