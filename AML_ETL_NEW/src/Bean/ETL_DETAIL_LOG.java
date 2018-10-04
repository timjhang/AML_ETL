package Bean;

import java.sql.Timestamp;
import java.sql.Date;

public class ETL_DETAIL_LOG {
	private String batch_no = null;
	private String central_no = null;
	private Date record_date = null;
	private String exe_status = null;
	private String exe_result = null;
	private String exe_result_description = null;
	private Timestamp start_datetime = null;
	private Timestamp end_datetime = null;

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

	public String getExe_status() {
		return exe_status;
	}

	public void setExe_status(String exe_status) {
		this.exe_status = exe_status;
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
}
