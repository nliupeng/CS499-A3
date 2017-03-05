package org.nliupeng.movieDemo;

import java.io.File;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The entry point for the Movie Average Ratings example,
 * which setup the Hadoop job with Map and Reduce Class
 */
public class MovieAnalyzer extends Configured implements Tool{
	
	protected static Map<Integer, String> MovieTitles = new HashMap<Integer, String>();
	
	/**
	 * Main function which calls the run method and passes the args using ToolRunner
	 * @param args Two arguments input and output file paths
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MovieAnalyzer(), args);
		System.exit(exitCode);
	}
 
	/**
	 * Run method which schedules the Hadoop Job
	 * @param args Arguments passed in main function
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s needs 3 arguments <input> <input2> <output>.\n"
					+ "<input>: TrainingRatings.txt\n <input2>: movie_titles.txt\n <output>: Output/",
					getClass().getSimpleName());

			return -1;
		}
	
		// Initializing and configuring the hadoop job,
		// which calculates the average ratings of each movie
		Job job = new Job();
		job.setJarByClass(MovieAnalyzer.class);
		job.setJobName("MovieRating");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(MovieMapClass.class);		
		job.setReducerClass(MovieReduceClass.class);
		
		
		// Initializing and configuring the movieTitle job,
		// which maps the ids of movies from an input file to its title as values
		Job movieTitleJob = new Job();
		movieTitleJob.setJobName("MovieTitle");
		
		FileInputFormat.addInputPath(movieTitleJob, new Path(args[1]));		
		movieTitleJob.setOutputFormatClass(null);
		
		movieTitleJob.setMapperClass(MovieTitleMapClass.class);
	
		
		// Wait for the jobs to complete and print if the jobs were successful or not
		int returnValue = job.waitForCompletion(true) ? 0:1;		
		int movieTitleReturnValue = movieTitleJob.waitForCompletion(true) ? 0:1;
		
		
		if(movieTitleJob.isSuccessful() && job.isSuccessful()) {
			System.out.println("Jobs were successful");
			
			// Calculate top 10 rated movies
			System.out.println("TOP TEN RATED MOVIES");
			printTopTenMovies(args);
			
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		} else if (!movieTitleJob.isSuccessful()) {
			System.out.println("MovieTitleJob was not successful");		
		}
		
		return returnValue & movieTitleReturnValue;
	}
	
	/**
	 * Reads the output file (movies and their average ratings) provided by the hadoop job
	 * and calculates the top 10 movies
	 */
	public void printTopTenMovies(String[] args) throws Exception {
		// Read input file
		Scanner sc = new Scanner(new File(args[2] + "/part-r-00000"));
		
		Comparator<Movie> comparator = new RatingComparator();
		PriorityQueue queue = new PriorityQueue(10, comparator);

		// Store data from file as items to a priority queue
		while (sc.hasNext()) {
			int movieId = sc.nextInt();
			float rating = sc.nextFloat();
			Movie movie = new Movie(movieId, rating);
			queue.add(movie);		
		}
		
		// Print out the top 10 from the queue	
		for (int i=0; i<10; i++) {
			Movie movie = (Movie)queue.poll();		
			System.out.println(MovieTitles.get(movie.getMovieId()));
		}
	}
	
}


class Movie {	
	private int movieId;
	private float rating;
	
	public Movie(int movieId, float rating) {
		this.movieId = movieId;
		this.rating = rating;
	}
	
	public int getMovieId() {
		return movieId;
	}
	
	public float getRating() {
		return rating;
	}
}

/**
 * Orders elements based on their rating
 */
class RatingComparator implements Comparator<Movie> {
	public int compare(Movie a, Movie b) {
		float ratingA = a.getRating();
		float ratingB = b.getRating();
		return ratingA < ratingB ? -1 : (ratingA > ratingB ? 1 : 0);	
	}
}



