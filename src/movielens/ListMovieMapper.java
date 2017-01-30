package movielens;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListMovieMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String columns[] = value.toString().split("\\|");
		LongWritable movieId = new LongWritable();
		movieId.set(Long.parseLong(columns[0]));

		String movieName = "mName" + "\t" + columns[1];
		//System.out.println("movieId :"+columns[0]);
		//System.out.println("movie name :"+columns[1]);
		context.write(movieId, new Text(movieName));
	}

}
