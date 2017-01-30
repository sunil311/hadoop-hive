package movielens;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieRatingDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		ToolRunner.run(configuration, new MovieRatingDriver(), args);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName("ReduceSideJoin Example");
		// input paths
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, ListRatingMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, ListMovieMapper.class);

		// output path
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setReducerClass(MovieRatingReducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

}