package org.nliupeng.movieDemo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieTitleMapClass extends Mapper<LongWritable, Text, IntWritable, Text>{

    private IntWritable movieId = new IntWritable();
    private Text title = new Text();
    
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line,",");
		
		while(st.hasMoreTokens()){
			movieId.set(Integer.parseInt(st.nextToken()));
			st.nextToken();		// skip year
			title.set(st.nextToken());
			
			// Map info in file to hashmap
			MovieAnalyzer.MovieTitles.put(movieId.get(), title.toString());
		}	
	}
	
}
