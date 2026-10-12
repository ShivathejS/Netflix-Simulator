package com.netflix.streamingservice.service;

import com.netflix.streamingservice.dto.StreamingResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamingService {

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final RedisTemplate<String, String> redisTemplate;


    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${aws.s3.presigned-url-expiry}")
    private long presignedUrlExpiry;

    @Value("${streaming.playlist-base-url}")
    private String playlistBaseUrl;

    //redis key for streaming URLs

    private static final String STREAMING_URL_CACHE_PREFIX = "streaming:url:";

    //Get streaming url for a movie
    //FLOW:
    // check redis cache for existing predesigned url
    //if cached, return immediately
    //if not cached, generate new predesigned url from s3
    // cache the url in redis
    // return streaming url
    public StreamingResponse getStreamingUrl(String movieId, String playlistKey){
        log.info("Getting streaming url for movie: {}", movieId);

        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;
        // check redis cache first
        String cachedUrl = redisTemplate.opsForValue().get(cacheKey);
        if(cachedUrl != null){
            log.info("Returning cached streaming URL for movie: {}", movieId);
            return new StreamingResponse(movieId, cachedUrl,
            "1080, 720, 480, 360p", presignedUrlExpiry);
        }
        // A master playlist has relative references to child playlists. Serve it
        // through this API so each child playlist can sign its own segments.
        log.info("Generating playlist URL for movie: {}", movieId);
        String playlistUrl = buildPlaylistEndpointUrl(movieId, playlistKey);

        // cache in redis for 55mins
        // 5mins less than actual expiry to avoid the edge cases
        redisTemplate.opsForValue().set(
               cacheKey,
               playlistUrl,
                55,
                TimeUnit.MINUTES
        );

        log.info("Streaming playlist URL generated and cached for movie: {}", movieId);
        return new StreamingResponse(
                movieId,
                playlistUrl,
                "1080, 720, 480, 360p",
                presignedUrlExpiry
        );
    }


    //this is the key method that makes everything secure.
    public String getSignedPlaylist(String movieId, String playlistPath){
      String normalizedPath = playlistPath.replace('\\', '/');
      String expectedPrefix = "encoded/" + movieId + "/";
      if (!normalizedPath.startsWith(expectedPrefix) || normalizedPath.contains("..")) {
          throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                  "Playlist path is not valid for this movie");
      }
      //get base path
      String basePath = normalizedPath.substring(0,
              normalizedPath.lastIndexOf('/')+1);
      String m3u8Content = readFromS3(normalizedPath);
      //rewrite each line that is a segment or a playlist reference
        String signedContent = rewriteM3u8SignedUrls(
                movieId, m3u8Content, basePath);
        return signedContent;
    }

    private String rewriteM3u8SignedUrls(String movieId, String m3u8Content,
                                         String basePath){
        StringBuilder rewritten = new StringBuilder();

        for(String line : m3u8Content.split("\n")){
           String trimmed = line.trim();
           // skip empty lines and comments
            if(trimmed.isEmpty() || trimmed.startsWith("#")){
                rewritten.append(line).append("\n");
                continue;
            }
            // Child playlists return to this endpoint so their segment links
            // are signed too. Video segments can be signed directly.
            String fullKey = basePath + trimmed;
            String url = trimmed.endsWith(".m3u8")
                    ? buildPlaylistEndpointUrl(movieId, fullKey)
                    : generatePresignedUrl(fullKey);
            rewritten.append(url).append("\n");
        }
        return rewritten.toString();
    }
    // read file content from s3
    private String readFromS3(String s3Key){
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);
             BufferedReader reader = new BufferedReader(new InputStreamReader(response))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Playlist not found", e);
        }
    }

    private String buildPlaylistEndpointUrl(String movieId, String playlistKey) {
        return UriComponentsBuilder.fromUriString(playlistBaseUrl)
                .pathSegment("api", "v1", "stream", movieId, "playlist")
                .queryParam("path", playlistKey)
                .build()
                .encode()
                .toUriString();
    }

private String generatePresignedUrl(String key){
    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .build();

    GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
            .signatureDuration(Duration.ofMinutes(presignedUrlExpiry))
            .getObjectRequest(getObjectRequest)
            .build();

    return s3Presigner.presignGetObject(presignRequest)
            .url().toString();
}
//invalidate cache streaming URL
    //called when video is re-encoded or updated

    public void invalidateCache(String movieId){
        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;
        redisTemplate.delete(cacheKey);
        log.info("Streaming URL cache invalidated for movie: {}", movieId);
    }

}
