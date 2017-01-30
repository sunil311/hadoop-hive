package movielens;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ListRatedMovieDriver extends Configured implements Tool {

	public static void main(String[] args) {
		try {
			int status = ToolRunner.run(new ListRatedMovieDriver(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf(
					"Usage: %s [generic options] <input1> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job = Job.getInstance();
		job.setJarByClass(ListRatedMovieDriver.class);
		job.setJobName("MapSideJoin Example");
		// input path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ListRatedMovieMapper.class);
		job.setNumReduceTasks(0);

		job.addCacheFile(new URI(
				"hdfs://localhost:54310/user/hduser/inputstore/movielens/u.data"));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
