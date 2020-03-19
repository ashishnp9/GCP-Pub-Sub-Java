package br.com.quintoandar.springcloudgcppubsubexample.publishers;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PubsubMessageOrBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;

public abstract class PubSubPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubPublisher.class);

    private final PubSubTemplate pubSubTemplate;

    protected PubSubPublisher(PubSubTemplate pubSubTemplate) {
        this.pubSubTemplate = pubSubTemplate;
    }

    protected abstract String topic();

    public void publish(String message) {
        LOGGER.info("publishing to topic [{}], message: [{}]", topic(), message);


        //pubSubTemplate.publish(topic(), message);


        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message))
            .putAttributes("ID","123").build();


        pubSubTemplate.publish(topic(),pubsubMessage);

    }

}
