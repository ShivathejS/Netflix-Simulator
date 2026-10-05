package com.netflix.streamingservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// consumed from Kafka topic: video.encoded

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VideoEncodedEvent {
    private String movieId;
    private String hlsUrl; //Master playlist url for streaming
    private String masterPlaylistKey; //S3 key of master.m3u8
    private boolean success;
    private String errorMessage;
}
