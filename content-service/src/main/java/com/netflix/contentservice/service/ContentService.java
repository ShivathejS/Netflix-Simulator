package com.netflix.contentservice.service;

import com.netflix.contentservice.dto.MovieRequest;
import com.netflix.contentservice.dto.MovieResponse;
import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;
import com.netflix.contentservice.model.VideoStatus;
//import com.netflix.contentservice.repository.ContentRepository;
import com.netflix.contentservice.repository.MovieRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ContentService {

        private final MovieRepository movieRepository;
        //Add a new movie to the catalog
    //Video is not uploaded yet
        public MovieResponse addMovie(MovieRequest request){
            log.info("Adding new movie: {}", request.getTitle());
            Movie movie = new Movie();
            movie.setTitle(request.getTitle());
            movie.setDescription(request.getDescription());
            movie.setGenre(Genre.valueOf(request.getGenre()));
            movie.setDirector(request.getDirector());
            movie.setCast(request.getCast());
            movie.setReleaseYear(request.getReleaseYear());
            movie.setRating(request.getRating());
            movie.setThumbnailUrl(request.getThumbnailUrl());
            movie.setDurationMinutes(request.getDurationMinutes());
            movie.setVideoStatus(VideoStatus.PENDING);
            //movie.setCreatedAt(request.getCreatedAt());

            Movie savedMovie = movieRepository.save(movie);
            log.info("Movie added with ID: {}", savedMovie.getId());
            return mapToResponse(savedMovie);
        }

        public List<MovieResponse> getAllMovies(){
            return movieRepository.findAll()
                    .stream()
                    .map(this::mapToResponse)
                    .collect(Collectors.toList());
        }

        //GET MOVIE BY ID
        public MovieResponse getMoviesById(String movieId){
            Movie movie = movieRepository.findById(movieId)
                    .orElseThrow(() -> new RuntimeException("Movie not found" +movieId ));
            return mapToResponse(movie);
        }

        //GET BY GENRE

        public List<MovieResponse> getMoviesByGenre (Genre genre){
            return movieRepository.findByGenre(genre)
                    .stream().map(this::mapToResponse)
                    .collect(Collectors.toList());
        }
        //Search movies by title
        public List<MovieResponse> searchMovies(String title){
            return movieRepository.findByTitleContainingIgnoreCase(title)
                    .stream()
                    .map(this::mapToResponse).collect(Collectors.toList());
        }

        public void updateVideoKey(String movieId, String videoKey){
            log.info("Updating video key for movie: {}",movieId);
            Movie movie = movieRepository.findById(movieId)
                    .orElseThrow(() -> new RuntimeException("Movie not found:" + movieId));

            movie.setVideoKey(videoKey);
            movie.setVideoStatus(VideoStatus.UPLOADED);
            movieRepository.save(movie);
        }

        public void updateHlsUrl(String movieId, String hlsUrl){
            log.info("Updating HLS URL for movie: {}", movieId);
            Movie movie = movieRepository.findById(movieId)
                    .orElseThrow(() -> new RuntimeException("Movie not found:" + movieId));
            movie.setHlsUrl(hlsUrl);
            movie.setVideoStatus(VideoStatus.READY);
            movieRepository.save(movie);

            log.info("Movie {} is now ready for streaming", movieId);
        }
        private MovieResponse mapToResponse(Movie movie){
            MovieResponse response =  new MovieResponse();
            response.setId(movie.getId());
            response.setTitle(movie.getTitle());
            response.setDescription(movie.getDescription());
            response.setGenre(movie.getGenre());
            response.setDirector(movie.getDirector());
            response.setCast(movie.getCast());
            response.setReleaseYear(movie.getReleaseYear());
            response.setRating(movie.getRating());
            response.setDurationMinutes(movie.getDurationMinutes());
            response.setVideoKey(movie.getVideoKey());
            response.setVideoStatus(movie.getVideoStatus());
            response.setHlsUrl(movie.getHlsUrl());
            response.setCreatedAt(movie.getCreatedAt());

            return response;
        }
}
