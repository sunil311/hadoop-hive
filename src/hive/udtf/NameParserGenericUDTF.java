package hive.udtf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class NameParserGenericUDTF extends GenericUDTF {

	private PrimitiveObjectInspector stringOI = null;
	private PrimitiveObjectInspector stringOI1 = null;
	private PrimitiveObjectInspector stringOI2 = null;
	private PrimitiveObjectInspector stringOI3 = null;
	private PrimitiveObjectInspector stringOI4 = null;
	private PrimitiveObjectInspector stringOI5 = null;
	List<String> listVlues = new ArrayList<String>();
	List<LogRecord> listLogVlues = new ArrayList<LogRecord>();
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

		// it can be dynamic - Check
		// input inspectors
		stringOI = (PrimitiveObjectInspector) args[0];
		stringOI1 = (PrimitiveObjectInspector) args[1];
		stringOI2 = (PrimitiveObjectInspector) args[2];
		stringOI3 = (PrimitiveObjectInspector) args[3];
		stringOI4 = (PrimitiveObjectInspector) args[4];
		stringOI5 = (PrimitiveObjectInspector) args[5];

		/*
		 * for(int i = 0; i < args.length; i++){ stringOI =
		 * (PrimitiveObjectInspector) args[i]; }
		 */

		// output inspectors -- an object with four fields!
		List<String> fieldNames = new ArrayList<String>();
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("unit_id");
		fieldNames.add("log_id");
		fieldNames.add("log_ts");
		fieldNames.add("element_id");
		fieldNames.add("element_value");
		fieldNames.add("transaction_type");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
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

		System.out.println("Calling process() of UDTF");

		/*
		 * final String name =
		 * stringOI.getPrimitiveJavaObject(record[0]).toString(); final String
		 * name1 = stringOI1.getPrimitiveJavaObject(record[1]).toString(); final
		 * String name2 =
		 * stringOI2.getPrimitiveJavaObject(record[2]).toString(); final String
		 * name3 = stringOI3.getPrimitiveJavaObject(record[3]).toString(); final
		 * String name4 =
		 * stringOI4.getPrimitiveJavaObject(record[4]).toString(); final String
		 * name5 = stringOI5.getPrimitiveJavaObject(record[5]).toString();
		 */

		LogRecord logRecordObj = new LogRecord();
		logRecordObj.setStrUnitID(stringOI.getPrimitiveJavaObject(record[0])
				.toString());
		logRecordObj.setStrLogID(stringOI1.getPrimitiveJavaObject(record[1])
				.toString());
		logRecordObj.setStrLogTS(stringOI2.getPrimitiveJavaObject(record[2])
				.toString());
		logRecordObj.setStrElementID(stringOI3
				.getPrimitiveJavaObject(record[3]).toString());
		logRecordObj.setStrElementValue(stringOI4.getPrimitiveJavaObject(
				record[4]).toString());
		logRecordObj.setStrTransactionType(stringOI5.getPrimitiveJavaObject(
				record[5]).toString());

		/*
		 * String name = ""; StringBuilder strBuilder = new StringBuilder();
		 * 
		 * for(int i = 0; i < record.length; i++){ name =
		 * stringOI.getPrimitiveJavaObject(record[i]).toString();
		 * strBuilder.append(name); if(i < record.length -1){
		 * strBuilder.append(","); } }
		 */

		// ArrayList<Object[]> results = processInputRecord(name);

		/*
		 * Iterator<Object[]> it = results.iterator();
		 * 
		 * while (it.hasNext()) { Object[] r = it.next(); forward(r); }
		 */

		// System.out.println(name + " " + ++counter);
		// listVlues.add(name + "," + name1 + "," + name2 + "," + name3 + "," +
		// name4);
		listLogVlues.add(logRecordObj);
		System.out.println(listLogVlues.toString());
	}

	public void doProcessing() {
		// Business/Processing Logic
		System.out
				.println("Start : doProcessing() of NameParserGenericUDTFNew");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date date1 = new Date();
		Date date2 = new Date();
		for (int i = 0; i < listLogVlues.size(); i++) {
			LogRecord logRecordObj1 = listLogVlues.get(i);
			String value1 = logRecordObj1.getStrLogTS();
			try {
				date1 = df.parse(value1);
			} catch (ParseException e) {
				e.printStackTrace();
			}

			for (int j = i + 1; j < listLogVlues.size(); j++) {
				LogRecord logRecordObj2 = listLogVlues.get(j);

				if (!logRecordObj1.getStrTransactionType().equals(
						logRecordObj2.getStrTransactionType())) {
					if (logRecordObj1.getStrElementID().equals(
							logRecordObj2.getStrElementID())) {
						listLogVlues.remove(logRecordObj1);
						listLogVlues.remove(logRecordObj2);
						i--;
						break;
					}
				}

				// Date logic to get the records of maximum time stamp
				String value2 = logRecordObj2.getStrLogTS();
				try {
					date2 = df.parse(value2);
				} catch (ParseException e) {
					e.printStackTrace();
				}

				if (date1.after(date2)) {
					listLogVlues.remove(logRecordObj2);
					i--;
					break;
				} else if (date1.before(date2)) {
					listLogVlues.remove(logRecordObj1);
					i--;
					break;
				}

			}

		}

		System.out.println("End : doProcessing() of NameParserGenericUDTFNew");

		/*
		 * Map<String, String> map = new HashMap<String, String>();
		 * for(LogRecord logRecord : listLogVlues){
		 * map.put(logRecord.getStrElementID() +
		 * logRecord.getStrTransactionType(), "DammyValue"); }
		 */

	}

	public void updateECC() {
		System.out.println("Start : updateECC() of NameParserGenericUDTFNew");

		for (int i = 0; i < listLogVlues.size(); i++) {
			LogRecord logRecordObj1 = listLogVlues.get(i);
			logRecordObj1.setStrTransactionType("ECC");
		}
		System.out.println("End : updateECC() of NameParserGenericUDTFNew");
	}

	@Override
	public void close() throws HiveException {
		System.out.println("Calling close() of UDTF");
		doProcessing();
		updateECC();
		System.out.println("Size of list : " + listLogVlues.size());
		for (LogRecord objLogRecord : listLogVlues) {
			String[] result = objLogRecord.toString().split(",");
			forward(result);
		}
		// forward(listVlues);
	}
}