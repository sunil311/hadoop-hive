package reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String columns[] = value.toString().split("\t");
		LongWritable userKey = new LongWritable();
		userKey.set(Long.parseLong(columns[0]));

		String userInfo = "info" + "\t" + columns[1];
		context.write(userKey, new Text(userInfo));
	}

}
