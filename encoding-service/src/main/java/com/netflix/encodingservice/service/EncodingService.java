package com.netflix.encodingservice.service;


import com.netflix.encodingservice.event.VideoEncodedEvent;
import com.netflix.encodingservice.event.VideoUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class EncodingService {
    private final S3Client s3Client;
    private final KafkaTemplate<String, VideoEncodedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${aws.s3.endpoint:}")
    private String s3Endpoint;

    @Value("${ffmpeg.path}")
    private String ffmpegPath;

    @Value("${encoding.base-path}")
    private String basePath;

    private static final String VIDEO_ENCODED_TOPIC =  "video.encoded";
    // video qualities to encode

    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
            new int[] {1920,5000,1080},
            new int[] {1280, 2800, 720},
            new int[] {854, 1200, 480},
            new int[] {640, 800, 360}
    );

    //Main encoding pipeline
    //Download vid from s3, encode to multiple qualities using FFmpeg
    //generate HLS playlist, create master playlist
    //upload all encoded files back to s3, publish encoded vid to Kafka
   public void encodeVideo(VideoUploadedEvent event){
       log.info("Starting encoding platform for movie:{}", event.getMovieId());
       //create a unique path for a movie

       String jobPath = basePath + "/" + event.getMovieId();

       try{
        // create temp directories
           Files.createDirectories(Paths.get(jobPath));
           Files.createDirectories(Paths.get(jobPath + "/encoded"));

           //Step 1: Download raw video from s3
           String localVideoPath = jobPath + "/raw_video.mp4";
           downloadFromS3(event.getVideoKey(), localVideoPath);
           log.info("Raw video downloaded to: {}", localVideoPath);

           //step 2&3: encode multiple qualities and generate hls
           for(int [] qualities: VIDEO_QUALITIES){
               int width = qualities[0];
               int bitrate = qualities[1];
               int height =  qualities[2];

               String qualityDir = jobPath + "/encoded/" + height + "p";
               Files.createDirectories(Paths.get(qualityDir));
               encodeToHLS(localVideoPath, qualityDir, width, height, bitrate);
               log.info("Encoded {}p successfully", height);
           }
           //generate master playlist
           String masterPlaylistPath = jobPath + "/encoded/master.m3u8";
           generateMasterPlaylist(masterPlaylistPath);
           log.info("Master Playlist generated");
           //Upload all resources to s3
           String encodedPrefix = "encoded/" + event.getMovieId() + "/";
           uploadEncodedFilesToS3(jobPath + "/encoded", encodedPrefix);
           log.info("All encoded files uploaded to S3");
           //Publish video encoded event
           String masterPlaylistKey = encodedPrefix + "master.m3u8";
           String hlsUrl = buildObjectUrl(masterPlaylistKey);
           VideoEncodedEvent encodedEvent = new VideoEncodedEvent(
                   event.getMovieId(),
                   hlsUrl,
                   masterPlaylistKey,
                   true,
                   null
           );
           kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), encodedEvent);
           log.info("VideoEncodedEvent published for move: {}", event.getMovieId());
       } catch (Exception e) {
            log.error("Encoding failed for movie: {} - {}", event.getMovieId(), e.getMessage());
            //publish failure event
           VideoEncodedEvent failureEvent = new VideoEncodedEvent(
                   event.getMovieId(),
                   null,
                   null,
                   false,
                   e.getMessage()
           );
           kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), failureEvent);
       }
       finally{
           cleanupTempFiles(jobPath);
       }
   }

   //Download file from s3 to local path
   private void downloadFromS3(String s3key, String localPath){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName).key(s3key)
                .build();
        s3Client.getObject(getObjectRequest, Paths.get(localPath));
    }

    private String buildObjectUrl(String key) {
        if (s3Endpoint == null || s3Endpoint.isBlank()) {
            return "https://" + bucketName + ".s3.amazonaws.com/" + key;
        }
        return s3Endpoint.replaceAll("/+$", "") + "/" + bucketName + "/" + key;
    }
    private void encodeToHLS(String inputPath,String outputDir,
                             int width, int height, int bitrate)throws IOException, InterruptedException {
      String playlistPath = outputDir + "/playlist.m3u8";
      String segmentPattern = outputDir + "/segment_%03d.ts";
      //FFmpeg command for hls encoding
        List<String> command = Arrays.asList(
                ffmpegPath,
                "-i", inputPath,      //input file
                "-vf", "scale=" + width + ":" + height,  //scale to res
                "-c:v", "libx264", // video codec
                "-b:v", bitrate + "k", //video bitrate
                "-c:a", "aac",  //audio coded
                "-b:a", "128k",  //audio bitrate
                "-hls_time" , "10",  //10 sec segments
                "-hls_list_size", "0",  //keep all segments
                "-hls_segment_filename" ,  segmentPattern, // segment naming
                "-f", "hls",                //output format
                playlistPath           //output playlist
         );
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode =  process.waitFor();
        if(exitCode!=0){
            throw new RuntimeException("FFmpeg encoding failed with exit code " + exitCode + ": " + output);
        }
    }
    private void generateMasterPlaylist(String masterPlaylistPath)throws IOException{
       StringBuilder master = new StringBuilder();
       master.append("#EXTM3U\n");
       master.append("#EXT-X-VERSION:3\n\n");
       //add each quality to master playlist
        int[][] qualities ={{1920, 5000, 1080}, {1280, 2800, 720},
                            {854, 1200, 480}, {640, 800, 360}};

        for(int [] q : qualities){
            int width = q[0];
            int bitrate = q[1];
            int height = q[2];

            master.append("#EXT-X-STREAM-INF:BANDWIDTH=")
                    .append(bitrate*1000)
                    .append(", RESOLUTION=").append(width).append("x").append(height)
                    .append(",CODECS=\"avc1.42e01e, mp4a.40.2\"\n");
            master.append(height).append("p/playlist.m3u8\n\n");

            //Files.writeString(Paths.get(masterPlaylistPath), master.toString());
        }
        Files.writeString(Paths.get(masterPlaylistPath), master.toString());
    }
    public void uploadEncodedFilesToS3(String localDir, String s3Prefix)throws IOException{
       File directory = new File(localDir);
       uploadDirectoryToS3(directory, localDir, s3Prefix);
    }
    private void uploadDirectoryToS3(File dir, String baseDir, String s3Prefix)throws IOException{
       for(File file : dir.listFiles()){
           if(file.isDirectory()){
               uploadDirectoryToS3(file, baseDir, s3Prefix);
           }
           else{
               String relativePath = file.getAbsolutePath()
                       .substring(baseDir.length()+1)
                       .replace("\\", "/");

               String s3Key = s3Prefix + relativePath;
               String contentType =file.getName().endsWith(".m3u8")
                       ? "application/x-mpegURL"
                       : "video/MP2T";

               PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                       .bucket(bucketName)
                       .key(s3Key)
                       .contentType(contentType).build();
               s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
               log.debug("Uploaded : {}", s3Key);
           }
       }
    }
    private void cleanupTempFiles(String jobPath){
       try{
           Path dirPath = Paths.get(jobPath);
           if(Files.exists(dirPath)){
               Files.walk(dirPath)
                       .sorted(java.util.Comparator.reverseOrder())
                       .map(Path::toFile)
                       .forEach(File::delete);

               log.info("Temp files cleaned up for job: {}", jobPath);
           }
       }
       catch (IOException e){
           log.warn("Failed to clean up temp files: {}", e.getMessage());
       }
    }
}
