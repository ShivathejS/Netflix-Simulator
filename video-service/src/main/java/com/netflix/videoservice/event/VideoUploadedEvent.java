package com.netflix.videoservice.event;

//Event published to kafka when a video is uploaded to s3
//Encoding service consume this to start FFmpeg processing
//video.uploaded

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VideoUploadedEvent {
    private String movieId;
    private String videoKey;
    private String bucketName;
    private String originalFileName;
    private long fileSizedBytes;

}
