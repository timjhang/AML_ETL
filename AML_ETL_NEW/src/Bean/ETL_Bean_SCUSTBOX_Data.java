package Bean;

import java.util.Date;

import Tool.ETL_Tool_ParseFileName;


public class ETL_Bean_SCUSTBOX_Data {

	
	// 報送單位
	private String central_no;
	// 檔案日期
	private Date record_date;
	// 檔名業務別
	private String file_type;
	// 行數
	private Integer row_count;
	// 本會代號
	private String domain_id;
	// 客戶統編
	private String party_number;
	// 異動代號
	private String change_code;
	// 保管箱隸屬行
	private String box_branch_code;
	// 租用箱號
	private String box_id;
	// 起租日
	private Date box_rent_date;
	// 戶況
	private String box_status;
	// 起租日
	private Date box_close_date;
	// 錯誤註記
	private String error_mark = "";// 預設無錯誤
	// 上傳批號(測試用)  // TODO V4
	private String upload_no;
	
	// Constructor
	public ETL_Bean_SCUSTBOX_Data(ETL_Tool_ParseFileName pfn) {
		
		this.central_no = pfn.getCentral_No();
		this.record_date = pfn.getRecord_Date();
		this.file_type = pfn.getFile_Type();
		this.upload_no = pfn.getUpload_no();  // TODO V4
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

	public String getFile_type() {
		return file_type;
	}

	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}

	public Integer getRow_count() {
		return row_count;
	}

	public void setRow_count(Integer row_count) {
		this.row_count = row_count;
	}

	public String getDomain_id() {
		return domain_id;
	}

	public void setDomain_id(String domain_id) {
		this.domain_id = domain_id;
	}

	public String getParty_number() {
		return party_number;
	}

	public void setParty_number(String party_number) {
		this.party_number = party_number;
	}

	public String getChange_code() {
		return change_code;
	}

	public void setChange_code(String change_code) {
		this.change_code = change_code;
	}
	
	public String getBox_branch_code() {
		return box_branch_code;
	}

	public void setBox_branch_code(String box_branch_code) {
		this.box_branch_code = box_branch_code;
	}
	
	public String getBox_id() {
		return box_id;
	}

	public void setBox_id(String box_id) {
		this.box_id = box_id;
	}

	public Date getBox_rent_date() {
		return box_rent_date;
	}

	public void setBox_rent_date(Date box_rent_date) {
		this.box_rent_date = box_rent_date;
	}

	public String getBox_status() {
		return box_status;
	}

	public void setBox_status(String box_status) {
		this.box_status = box_status;
	}
	
	public Date getBox_close_date() {
		return box_close_date;
	}

	public void setBox_close_date(Date box_close_date) {
		this.box_close_date = box_close_date;
	}
	
	public String getError_mark() {
		return error_mark;
	}

	public void setError_mark(String error_mark) {
		this.error_mark = error_mark;
	}

	// TODO V4  Start
	public String getUpload_no() {
		return upload_no;
	}

	public void setUpload_no(String upload_no) {
		this.upload_no = upload_no;
	}
	// TODO V4  End
}
