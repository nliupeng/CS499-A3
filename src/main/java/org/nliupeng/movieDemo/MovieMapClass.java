package org.nliupeng.movieDemo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map Class which extends MapReduce.Mapper class
 * Map reads an input line at a time, and tokenizes based on comma delimiter.
 * It maps tokens with:
 * Key: movieID and Value: rating
 * the latter to be consumed by the ReduceClass
 */
public class MovieMapClass extends Mapper<LongWritable, Text, IntWritable, FloatWritable>{

    private IntWritable movieId = new IntWritable();
    private FloatWritable rating = new FloatWritable();
    
    /**
     * map function of Mapper parent class takes a line of text at a time and
     * splits to tokens and passes to the context as key: movieId, and value: rating.
     * The input text is composed of [movieId, userId, rating] 
     */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line,",");
		
		while(st.hasMoreTokens()){
			movieId.set(Integer.parseInt(st.nextToken()));
			st.nextToken();		// skip userId
			rating.set(Float.parseFloat(st.nextToken()));
			context.write(movieId, rating);
		}	
	}
	
}
