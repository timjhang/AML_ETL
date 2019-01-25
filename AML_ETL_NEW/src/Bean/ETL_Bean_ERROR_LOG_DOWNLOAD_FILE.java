package Bean;

import java.util.ArrayList;
import java.util.List;

public class ETL_Bean_ERROR_LOG_DOWNLOAD_FILE {
	private String file_name;
	private List<ETL_Bean_ERROR_LOG_DOWNLOAD> body;
	
	public ETL_Bean_ERROR_LOG_DOWNLOAD_FILE() {
		this.body = new ArrayList<ETL_Bean_ERROR_LOG_DOWNLOAD>();
	}
	
	public String getFile_name() {
		return file_name;
	}
	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}
	public List<ETL_Bean_ERROR_LOG_DOWNLOAD> getBody() {
		return body;
	}
	public void setBody(List<ETL_Bean_ERROR_LOG_DOWNLOAD> body) {
		this.body = body;
	}
	
	
}
