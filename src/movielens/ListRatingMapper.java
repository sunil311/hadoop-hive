package movielens;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListRatingMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String columns[] = value.toString().split("\t");
		LongWritable movieId = new LongWritable();
		movieId.set(Long.parseLong(columns[1]));

		//System.out.println("movieId :"+columns[1]);
		//System.out.println("rating :"+columns[2]);
		
		String rating = "rating" + "\t" + columns[2];
		context.write(movieId, new Text(rating));

	}
}
