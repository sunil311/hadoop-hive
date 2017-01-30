package movielens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class MovieRatingVO {

	private Text movieName;
	private DoubleWritable rating;

	public Text getMovieName() {
		return movieName;
	}

	public void setMovieName(Text movieName) {
		this.movieName = movieName;
	}

	public DoubleWritable getRating() {
		return rating;
	}

	public void setRating(DoubleWritable rating) {
		this.rating = rating;
	}

	public void readFields(DataInput in) throws IOException {
		movieName.readFields(in);
		rating.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		movieName.write(out);
		rating.write(out);
	}

	@Override
	public String toString() {
		return movieName.toString() + "\t" + rating.toString();
	}

}
