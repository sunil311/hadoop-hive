package movielens;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ListUsersReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> it = value.iterator();
		int ratingSum = 0;
		while (it.hasNext()) {
			IntWritable rating = (IntWritable) it.next();
			ratingSum = ratingSum + rating.get();
		}
		context.write(key, new IntWritable(ratingSum));
	}
}
