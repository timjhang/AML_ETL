package Bean;

import java.sql.Date;
import java.sql.Timestamp;

public class ETL_FILE_LOG {
	private String batch_no = null;
	private String central_no = null;
	private Date record_date = null;
	private String file_name = null;
	private Timestamp start_datetime = null;
	private Timestamp end_datetime = null;
	private String exe_result = null;
	private String exe_result_description = null;
	private String src_file = null;

	public String getBatch_no() {
		return batch_no;
	}

	public void setBatch_no(String batch_no) {
		this.batch_no = batch_no;
	}

	public String getCentral_no() {
		return central_no;
	}

	public void setCentral_no(String central_no) {
		this.central_no = central_no;
	}

	public Date getRecord_date() {
		return record_date;
	}

	public void setRecord_date(Date record_date) {
		this.record_date = record_date;
	}

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public Timestamp getStart_datetime() {
		return start_datetime;
	}

	public void setStart_datetime(Timestamp start_datetime) {
		this.start_datetime = start_datetime;
	}

	public Timestamp getEnd_datetime() {
		return end_datetime;
	}

	public void setEnd_datetime(Timestamp end_datetime) {
		this.end_datetime = end_datetime;
	}

	public String getExe_result() {
		return exe_result;
	}

	public void setExe_result(String exe_result) {
		this.exe_result = exe_result;
	}

	public String getExe_result_description() {
		return exe_result_description;
	}

	public void setExe_result_description(String exe_result_description) {
		this.exe_result_description = exe_result_description;
	}

	public String getSrc_file() {
		return src_file;
	}

	public void setSrc_file(String src_file) {
		this.src_file = src_file;
	}

}
