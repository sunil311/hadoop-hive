package movielens;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListRatedMovieMapper extends
		Mapper<LongWritable, Text, IntWritable, NullWritable> {

	// movie map to keep the movieId-rating
	private Map<Integer, Integer> movieRatingMap = new HashMap<Integer, Integer>();
	NullWritable nullValue = NullWritable.get();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String columns[] = value.toString().split("\\|");

		if (movieRatingMap.get(Integer.parseInt(columns[0])) != null) {
			context.write(new IntWritable(Integer.parseInt(columns[0])),
					nullValue);
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// loading user map in context
		loadUserInMemory(context);
	}

	private void loadUserInMemory(Context context) {
		// user.log is in distributed cache
		BufferedReader br = null;
		try {

			br = new BufferedReader(new FileReader("u.data"));
			String line;
			while ((line = br.readLine()) != null) {
				String columns[] = line.split("\t");
				movieRatingMap.put(Integer.parseInt(columns[1]),
						Integer.parseInt(columns[2]));
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

	}
}
