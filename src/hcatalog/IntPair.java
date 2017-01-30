package hcatalog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {

	private int year;
	private int month;

	IntPair() {

	}

	IntPair(int year, int month) {
		this.month = month;
		this.year = year;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
	}

	@Override
	public String toString() {
		return "IntPair [year=" + year + ", month=" + month + "]";
	}

	public int compareTo(IntPair intPair) {
		return 1;
	}

}
