package movielens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RatingMinMaxVO implements Writable {
	private int minRating;
	private int maxRating;

	public int getMinRating() {
		return minRating;
	}

	public void setMinRating(int minRating) {
		this.minRating = minRating;
	}

	public int getMaxRating() {
		return maxRating;
	}

	public void setMaxRating(int maxRating) {
		this.maxRating = maxRating;
	}

	public void readFields(DataInput in) throws IOException {
		minRating = in.readInt();
		maxRating = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(minRating);
		out.writeInt(maxRating);
	}

	@Override
	public String toString() {
		return maxRating + "\t" + minRating;
	}
}
