package Bean;

import java.util.Date;

import Tool.ETL_Tool_ParseFileName;


public class ETL_Bean_AGENT_Data {

	
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
	// 姓名
	private String party_last_name_1;
	// 出生年月日
	private Date date_of_birth;
	// 國籍
	private String nationality_code;
	// 通訊地址
	private String address_line_1;	
	// 錯誤註記
	private String error_mark = "";// 預設無錯誤
	// 上傳批號(測試用)  // TODO V4
	private String upload_no;
	
	// Constructor
	public ETL_Bean_AGENT_Data(ETL_Tool_ParseFileName pfn) {
		
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

	public String getAddress_line_1() {
		return address_line_1;
	}

	public void setAddress_line_1(String address_line_1) {
		this.address_line_1 = address_line_1;
	}

	public Date getDate_of_birth() {
		return date_of_birth;
	}

	public void setDate_of_birth(Date date_of_birth) {
		this.date_of_birth = date_of_birth;
	}

	public String getNationality_code() {
		return nationality_code;
	}

	public void setNationality_code(String nationality_code) {
		this.nationality_code = nationality_code;
	}

	public String getParty_last_name_1() {
		return party_last_name_1;
	}

	public void setParty_last_name_1(String party_last_name_1) {
		this.party_last_name_1 = party_last_name_1;
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
