package tw.com.pershing.databean;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="Response")
public class ETLresponse {
	
	private String msg;
	private List<String> logs;
	private String error;
	private String fileInfo;

	@XmlElement(name="msg")
	public String getMsg() {
		return msg;
	}
	
	public List<String> getLogs() {
		return logs;
	}

	@XmlElement(name="logs")
	public void setLogs(List<String> logs) {
		this.logs = logs;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}
	
	@XmlElement(name="errorMsg")
	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	@XmlElement(name="fileInfo")
	public String getFileInfo() {
		return fileInfo;
	}

	public void setFileInfo(String fileInfo) {
		this.fileInfo = fileInfo;
	}

}
