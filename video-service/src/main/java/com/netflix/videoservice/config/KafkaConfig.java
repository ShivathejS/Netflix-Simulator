package com.netflix.videoservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.TopicForRetryable;

@Configuration

public class KafkaConfig {

    //published when vid uploaded to s3
    //Encoding service consumes this
    @Bean
    public NewTopic videoUploadedTopic(){
        return TopicBuilder.name("video.uploaded")
                .partitions(3)
                .replicas(1)
                .build();
    }
    //published when encoding is complete
    @Bean
    public NewTopic videoEncodedTopic(){
        return TopicBuilder.name("video.encoded")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
