package com.netflix.contentservice.model;

//Tracks the video processing lifecycle
// PENDING, UPLOAED, ENCODING, READY

public enum VideoStatus {
    PENDING, //ADDED BUT NOT UPLOADED
    UPLOADED, //RAW VID TO S3
    ENCODING, //FFmpeg encoding the video
    ENCODED,  // Encoding complete
    READY,  //HLS playlist ready - can be streamed
    FAILED //Encoding failed
}
