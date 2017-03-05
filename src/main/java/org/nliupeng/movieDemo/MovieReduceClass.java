package org.nliupeng.movieDemo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class which is executed after the map class and takes
 * key(movieId) and corresponding values(ratings), sums all the values and divides
 * by the total number of the same movieId instances to get the average rating, and writes the
 * movieId along with the corresponding average rating
 */
public class MovieReduceClass extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>{

	/**
	 * Method which performs the reduce operation and obtains the average rating  
	 * of each movie before passing it to be stored in output
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
	
		int total = 0;
		float average = 0;
		float totalAverage = 0;
		
		Iterator<FloatWritable> valuesIt = values.iterator();
		
		while(valuesIt.hasNext()){
			average += valuesIt.next().get();
			total++;
		}
		
		totalAverage = average / total;
		context.write(key, new FloatWritable(totalAverage));
	}	
	
}