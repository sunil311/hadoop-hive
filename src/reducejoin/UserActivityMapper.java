package reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserActivityMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String columns[] = value.toString().split("\\|");
		LongWritable userKey = new LongWritable();
		userKey.set(Long.parseLong(columns[1]));
		String userActivity = "activity" + "\t" + columns[3];
		context.write(userKey, new Text(userActivity));

	}
}
