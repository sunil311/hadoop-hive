package movielens;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, RatingMinMaxVO> {
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {

		int maxValue = 0;
		int minValue = 5;
		Iterator<IntWritable> itr = value.iterator();

		while (itr.hasNext()) {
			IntWritable rating = itr.next();
			maxValue = Math.max(maxValue, rating.get());
			minValue = Math.min(minValue, rating.get());

		}
		RatingMinMaxVO minMax = new RatingMinMaxVO();
		minMax.setMaxRating(maxValue);
		minMax.setMinRating(minValue);
		context.write(key, minMax);
	}
}
