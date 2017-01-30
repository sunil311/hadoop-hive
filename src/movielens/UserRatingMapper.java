package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRatingMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String columns[] = value.toString().split("\t");
		IntWritable userId = new IntWritable();
		userId.set(Integer.parseInt(columns[0]));
		IntWritable rating = new IntWritable();
		rating.set(Integer.parseInt(columns[2]));
		context.write(userId, rating);
	}
}
