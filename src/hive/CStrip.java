package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.commons.lang.StringUtils;

public class CStrip extends UDF {
	private Text result = new Text();

	public Text evaluate(Text str, String stripChars) {

		if (str == null) {

			return null;

		}

		result.set(StringUtils.strip(str.toString(), stripChars));

		return result;

	}

	public Text evaluate(Text str) {

		if (str == null) {

			return null;

		}

		result.set(StringUtils.strip(str.toString()));

		return result;

	}
}
