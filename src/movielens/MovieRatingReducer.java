package movielens;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingReducer extends
		Reducer<LongWritable, Text, Text, DoubleWritable> {
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Iterator<Text> movieRatingItr = values.iterator();
		int tmpRating = 0;
		int count = 0;
		String movieName = "";
		while (movieRatingItr.hasNext()) {
			Text movieRating = movieRatingItr.next();
			String movieRatingArr[] = movieRating.toString().split("\t");

			if ("rating".equalsIgnoreCase(movieRatingArr[0])) {
				count++;
				tmpRating = tmpRating + Integer.parseInt(movieRatingArr[1]);
			} else if ("mName".equalsIgnoreCase(movieRatingArr[0])) {
				movieName = movieRatingArr[1];
			}
		}
		DoubleWritable ratingWritable = new DoubleWritable();
		System.out.println("movieName :"+movieName);
		System.out.println("rating :"+tmpRating);
		System.out.println("count :"+count);
		ratingWritable.set(tmpRating / count);
		context.write(new Text(movieName), ratingWritable);
	}
}
