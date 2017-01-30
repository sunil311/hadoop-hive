package hcatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;




public class HcatalogReducer extends Reducer<IntPair, IntWritable, NullWritable, HCatRecord> {
	@SuppressWarnings("deprecation")
	public void reduce(IntPair key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		int count = 0; // records counter for particular year-month
		for (Iterator<IntWritable> iterator = value.iterator(); iterator
				.hasNext();) {
			count++;
		}

		// define output record schemsa
		List<HCatFieldSchema> columns = new ArrayList<HCatFieldSchema>(3);
		columns.add(new HCatFieldSchema("year", HCatFieldSchema.Type.INT, ""));
		columns.add(new HCatFieldSchema("month", HCatFieldSchema.Type.INT, ""));
		columns.add(new HCatFieldSchema("flightCount",
				HCatFieldSchema.Type.INT, ""));
		HCatSchema schema = new HCatSchema(columns);
		HCatRecord record = new DefaultHCatRecord(3);

		record.setInteger("year", schema, key.getYear());
		record.set("month", schema, key.getMonth());
		record.set("flightCount", schema, count);
		context.write(null, record);
	}

}
