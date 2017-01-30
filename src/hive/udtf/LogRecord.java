package hive.udtf;

public class LogRecord {
	private String strUnitID;
	private String strLogID;
	private String strLogTS;
	private String strElementID;
	private String strElementValue;
	private String strTransactionType;

	public LogRecord() {

	}

	public LogRecord(String strUnitID, String strLogID, String strLogTS,
			String strElementID, String strElementValue,
			String strTransactionType) {
		super();
		this.strUnitID = strUnitID;
		this.strLogID = strLogID;
		this.strLogTS = strLogTS;
		this.strElementID = strElementID;
		this.strElementValue = strElementValue;
		this.strTransactionType = strTransactionType;
	}

	public String getStrUnitID() {
		return strUnitID;
	}

	public void setStrUnitID(String strUnitID) {
		this.strUnitID = strUnitID;
	}

	public String getStrLogID() {
		return strLogID;
	}

	public void setStrLogID(String strLogID) {
		this.strLogID = strLogID;
	}

	public String getStrLogTS() {
		return strLogTS;
	}

	public void setStrLogTS(String strLogTS) {
		this.strLogTS = strLogTS;
	}

	public String getStrElementID() {
		return strElementID;
	}

	public void setStrElementID(String strElementID) {
		this.strElementID = strElementID;
	}

	public String getStrElementValue() {
		return strElementValue;
	}

	public void setStrElementValue(String strElementValue) {
		this.strElementValue = strElementValue;
	}

	public String getStrTransactionType() {
		return strTransactionType;
	}

	public void setStrTransactionType(String strTransactionType) {
		this.strTransactionType = strTransactionType;
	}

	@Override
	public String toString() {
		/*
		 * return "LogRecord [strUnitID=" + strUnitID + ", strLogID=" + strLogID
		 * + ", strLogTS=" + strLogTS + ", strElementID=" + strElementID +
		 * ", strElementValue=" + strElementValue + ", strTransactionType=" +
		 * strTransactionType + "]";
		 */

		return strUnitID + "," + strLogID + "," + strLogTS + "," + strElementID
				+ "," + strElementValue + "," + strTransactionType;
	}

}
