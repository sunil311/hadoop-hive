package reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UserVO implements Writable {

	private Text userName;
	public Text getUserName() {
		return userName;
	}

	public void setUserName(Text userName) {
		this.userName = userName;
	}

	public Text getActivityURL() {
		return activityURL;
	}

	public void setActivityURL(Text activityURL) {
		this.activityURL = activityURL;
	}

	private Text activityURL;

	public void readFields(DataInput in) throws IOException {
		userName.readFields(in);
		activityURL.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		userName.write(out);
		activityURL.write(out);
	}

	@Override
	public String toString() {
		return userName.toString() + "\t" + activityURL.toString();
	}

}
