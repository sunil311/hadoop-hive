package wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> its =values.iterator();
		while (its.hasNext()) {
			IntWritable object =  its.next();
			sum += object.get();
		}
		context.write(key, new IntWritable(sum));
	}

}
