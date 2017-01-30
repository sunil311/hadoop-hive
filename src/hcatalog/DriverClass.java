package hcatalog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class DriverClass extends Configured implements Tool {
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(DriverClass.class);

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance();
		job.setJarByClass(DriverClass.class);
		job.setMapperClass(HcatalogMapper.class);
		job.setReducerClass(HcatalogReducer.class);
		System.out.println("gupta");
		HCatInputFormat.setInput(job, "airln", "ontimeperf");
		job.setInputFormatClass(HCatInputFormat.class);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		System.out.println("mca");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		job.setOutputFormatClass(HCatOutputFormat.class);

		HCatOutputFormat.setOutput(job,
				OutputJobInfo.create("airln", "flight_count", null));
		HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
		HCatOutputFormat.setSchema(job, s);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DriverClass(), args);
		System.out.println("exit code :"+exitCode);
		System.exit(exitCode);
	}
}
