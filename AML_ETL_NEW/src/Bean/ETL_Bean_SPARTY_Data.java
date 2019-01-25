package Bean;

import java.util.Date;

import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_SPARTY_Data {
	
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
	// 客戶姓名
	private String party_last_name_1;
	// 客戶生日
	private Date date_of_birth;
	// 客戶國籍
	private String nationality_code;
	// 客戶類型
	private String entity_type;
	// 客戶職業/行業
	private String occupation_code;
	// 客戶手機
	private String cell_phone_number;
	// 客戶電話
	private String phone_number;
	// 客戶戶籍地址
	private String address_line_1;
	// 客戶通訊地址
	private String address_line_2;
	// 錯誤註記
	private String error_mark = "";// 預設無錯誤
	// 上傳批號(測試用)  // TODO V4
	private String upload_no;
	
	// Constructor
	public ETL_Bean_SPARTY_Data(ETL_Tool_ParseFileName pfn) {
		
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
	
	public String getParty_last_name_1() {
		return party_last_name_1;
	}

	public void setParty_last_name_1(String party_last_name_1) {
		this.party_last_name_1 = party_last_name_1;
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
	
	public String getEntity_type() {
		return entity_type;
	}

	public void setEntity_type(String entity_type) {
		this.entity_type = entity_type;
	}
	
	public String getOccupation_code() {
		return occupation_code;
	}

	public void setOccupation_code(String occupation_code) {
		this.occupation_code = occupation_code;
	}
	
	public String getCell_phone_number() {
		return cell_phone_number;
	}

	public void setCell_phone_number(String cell_phone_number) {
		this.cell_phone_number = cell_phone_number;
	}
	
	public String getPhone_number() {
		return phone_number;
	}

	public void setPhone_number(String phone_number) {
		this.phone_number = phone_number;
	}
	
	public String getAddress_line_1() {
		return address_line_1;
	}

	public void setAddress_line_1(String address_line_1) {
		this.address_line_1 = address_line_1;
	}
	
	public String getAddress_line_2() {
		return address_line_2;
	}

	public void setAddress_line_2(String address_line_2) {
		this.address_line_2 = address_line_2;
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
