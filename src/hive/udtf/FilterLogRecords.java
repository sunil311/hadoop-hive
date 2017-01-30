package hive.udtf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class FilterLogRecords extends GenericUDTF {

	private PrimitiveObjectInspector stringOI = null;
	private PrimitiveObjectInspector stringOI1 = null;
	private PrimitiveObjectInspector stringOI2 = null;
	private PrimitiveObjectInspector stringOI3 = null;
	private PrimitiveObjectInspector stringOI4 = null;
	List<String> listVlues = new ArrayList<String>();
	int counter = 0;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {

		System.out.println("Calling initialize() of UDTF");

		/*
		 * if (args.length != 1) { throw new UDFArgumentException(
		 * "NameParserGenericUDTF() takes exactly one argument"); }
		 */

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(
					"NameParserGenericUDTF() takes a string as a parameter");
		}

		// input inspectors
		stringOI = (PrimitiveObjectInspector) args[0];
		stringOI1 = (PrimitiveObjectInspector) args[1];
		stringOI2 = (PrimitiveObjectInspector) args[2];
		stringOI3 = (PrimitiveObjectInspector) args[3];
		stringOI4 = (PrimitiveObjectInspector) args[4];

		// output inspectors -- an object with four fields!
		List<String> fieldNames = new ArrayList<String>(2);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
		fieldNames.add("unit_id");
		fieldNames.add("element_id");
		fieldNames.add("log_id");
		fieldNames.add("log_ts");
		fieldNames.add("transaction_type");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	public ArrayList<Object[]> processInputRecord(String name) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();

		// ignoring null or empty input
		if (name == null || name.isEmpty()) {
			return result;
		}

		String[] tokens = name.split("\\s+");

		if (tokens.length == 2) {
			result.add(new Object[] { tokens[0], tokens[1] });
		} else if (tokens.length == 4 && tokens[1].equals("and")) {
			result.add(new Object[] { tokens[0], tokens[3] });
			result.add(new Object[] { tokens[2], tokens[3] });
		}

		return result;
	}

	@Override
	public void process(Object[] record) throws HiveException {

		final String name = stringOI.getPrimitiveJavaObject(record[0])
				.toString();
		final String name1 = stringOI1.getPrimitiveJavaObject(record[1])
				.toString();
		final String name2 = stringOI2.getPrimitiveJavaObject(record[2])
				.toString();
		final String name3 = stringOI3.getPrimitiveJavaObject(record[3])
				.toString();
		final String name4 = stringOI4.getPrimitiveJavaObject(record[4])
				.toString();

		// ArrayList<Object[]> results = processInputRecord(name);

		/*
		 * Iterator<Object[]> it = results.iterator();
		 * 
		 * while (it.hasNext()) { Object[] r = it.next(); forward(r); }
		 */
		System.out.println("Calling process() of UDTF");

		// System.out.println(name + " " + ++counter);
		listVlues.add(name + "," + name1 + "," + name2 + "," + name3 + ","
				+ name4);
		System.out.println(listVlues.toString());
	}

	public void doProcessing() {
		// listVlues.add(0, "Sunil Sir");
	}

	@Override
	public void close() throws HiveException {
		System.out.println("Calling close() of UDTF");
		// doProcessing();String
		System.out.println("Size of list : " + listVlues.size());
		for (String str : listVlues) {
			String[] result = str.split(",");
			forward(result);
		}
		// forward(listVlues);
	}
}
