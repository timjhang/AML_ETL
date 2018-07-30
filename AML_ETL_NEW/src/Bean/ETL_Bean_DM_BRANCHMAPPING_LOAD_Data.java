package Bean;

import java.util.Date;

import Tool.ETL_Tool_DM_ParseFileName;

public class ETL_Bean_DM_BRANCHMAPPING_LOAD_Data {

	private String old_branch_code;// 舊分支機構代號
	private String new_branch_code;// 新分支機構代號
	private Date record_date;// 檔案日期
	private String batch_no;// 批次編號
	private Date create_date;// 創建時間
	private Integer row_count = 0;// 行數
	private String error_mark = "";// 錯誤註記

	public ETL_Bean_DM_BRANCHMAPPING_LOAD_Data(ETL_Tool_DM_ParseFileName pfn) {
		this.record_date = pfn.getRecord_date();// 檔案日期
		this.batch_no = pfn.getBatch_no();// 批次編號
	}

	public String getOld_branch_code() {
		return old_branch_code;
	}

	public void setOld_branch_code(String old_branch_code) {
		this.old_branch_code = old_branch_code;
	}

	public String getNew_branch_code() {
		return new_branch_code;
	}

	public void setNew_branch_code(String new_branch_code) {
		this.new_branch_code = new_branch_code;
	}

	public Date getRecord_date() {
		return record_date;
	}

	public void setRecord_date(Date record_date) {
		this.record_date = record_date;
	}

	public String getBatch_no() {
		return batch_no;
	}

	public void setBatch_no(String batch_no) {
		this.batch_no = batch_no;
	}

	public Date getCreate_date() {
		return create_date;
	}

	public void setCreate_date(Date create_date) {
		this.create_date = create_date;
	}

	public Integer getRow_count() {
		return row_count;
	}

	public void setRow_count(Integer row_count) {
		this.row_count = row_count;
	}

	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}
	
	
}
