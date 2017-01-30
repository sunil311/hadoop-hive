package reducejoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserActivityReducer extends
		Reducer<LongWritable, Text, LongWritable, UserVO> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Iterator<Text> userActivityItr = values.iterator();
		UserVO userVO = new UserVO();

		String userName = "";
		String activityURL = "";
		while (userActivityItr.hasNext()) {
			Text userActivity = userActivityItr.next();
			String userInfo[] = userActivity.toString().split("\t");

			if ("info".equalsIgnoreCase(userInfo[0])) {
				userName = userName + userInfo[1];
			} else if ("activity".equalsIgnoreCase(userInfo[0])) {
				activityURL = activityURL + userInfo[1] + "\t";
			}
		}
		userVO.setUserName(new Text(userName));
		userVO.setActivityURL(new Text(activityURL));
		context.write(key, userVO);
	}

}
