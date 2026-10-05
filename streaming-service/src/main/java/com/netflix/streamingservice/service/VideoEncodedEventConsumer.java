package com.netflix.streamingservice.service;


import com.netflix.streamingservice.event.VideoEncodedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor

public class VideoEncodedEventConsumer {
    private final RedisTemplate<String, String > redisTemplate;
    private static final String MASTER_PLAYLIST_KEY_PREFIX = "streaming:playlist:";
    //listens to video.encoded kafka topic
    //stores master playlist key on redis when encoding is complete

    @KafkaListener(
            topics = "video.encoded",
            groupId = "streaming-service-group",
            autoStartup = "${streaming.kafka.listener-enabled}"
    )
    public void consumeVideoEncodedEvent(VideoEncodedEvent event){
        log.info("Consumed VideoEncodedEvent for movie:{} success",
                event.getMovieId(), event.isSuccess());
        if(event.isSuccess()){
            //store master playlist key in redis
            String cacheKey =  MASTER_PLAYLIST_KEY_PREFIX + event.getMovieId();
            redisTemplate.opsForValue().set(cacheKey, event.getMasterPlaylistKey());
            log.info("Master playlist key stored in Redis for movie: {}", event.getMovieId());
        }
        else{
            log.info("Encoding failed for movie: {} - {} ",
                    event.getMovieId(), event.getErrorMessage());
        }
    }

}
