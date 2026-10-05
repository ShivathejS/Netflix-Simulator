package com.netflix.encodingservice.service;

import com.netflix.encodingservice.event.VideoUploadedEvent;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@Slf4j
public class VideoEventConsumer {
    private final EncodingService encodingService;
    //listens to video uploaded kafka
    //triggered when vid service uploads raw vid to s3
    //vid service - s3 - kafka(vid.uploaded

@KafkaListener(
        topics = "video.uploaded",
        groupId = "encoding-service-group",
        autoStartup = "${encoding.kafka.listener-enabled}")
public void consumeVideoUploadedEvent(VideoUploadedEvent event){
    log.info("Consumed VideoUploadedEvent for movie: {} file: {} ",
           event.getMovieId(), event.getOriginalFileName());
    try{
        encodingService.encodeVideo(event);
    }
    catch(Exception e){
        log.info("Failed to process encoding for movie: {} - {}",
                event.getMovieId(), e.getMessage());
    }
}

}
