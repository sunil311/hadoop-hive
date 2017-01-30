package movielens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RatingVo implements Writable {
	private int rating;

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}

	public void readFields(DataInput in) throws IOException {
		rating = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(rating);
	}
	@Override
	public String toString() {
		return rating+"";
	}

}
