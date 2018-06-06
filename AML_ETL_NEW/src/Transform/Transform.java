package Transform;

import Bean.ETL_Bean_LogData;

public abstract class Transform implements Runnable {

	ETL_Bean_LogData logData;
	
	public Transform() {
		
	}
	
	public Transform(ETL_Bean_LogData logData) {
		this.logData = logData;
	}
	
	abstract void trans_File();
	
	@Override
	public void run() {
		trans_File();
	}
	
}
