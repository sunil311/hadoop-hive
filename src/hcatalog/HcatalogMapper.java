package hcatalog;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;

@SuppressWarnings("rawtypes")
public class HcatalogMapper extends
		Mapper<WritableComparable, HCatRecord, IntPair, IntWritable> {

	@Override
	protected void map(
			WritableComparable key,
			HCatRecord value,
			Mapper<WritableComparable, HCatRecord, IntPair, IntWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			System.out.println(" Mapper invoked....");
			System.out.println(" Mapper ...." + value.toString());
			System.out.println("Mapper Started");
			HCatSchema schema = HCatBaseInputFormat.getTableSchema(context
					.getConfiguration());

			Integer year = new Integer(value.getInteger("year", schema));
			Integer month = new Integer(value.getInteger("month", schema));
			Integer DayofMonth = value.getInteger("dayofmonth", schema);
			System.out.println("Data read from input table " + year + "||"
					+ month + "||" + DayofMonth);
			context.write(new IntPair(year, month), new IntWritable(DayofMonth));
			System.out.println("Mapper Completed");
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
}