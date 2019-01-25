package Bean;

import java.util.Date;
import java.sql.Time;
import java.sql.Timestamp;
import Tool.ETL_Tool_ParseFileName;


public class ETL_Bean_SCUSTBOXOPEN_Data {

	
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
	// 保管箱所在行
	private String box_branch_code;
	// 租用箱號
	private String box_id;
	// 起租日
	private Date box_rent_date;
	//開箱日期
	private Date box_open_date;
	// 開箱人員統編
	private String box_opener_id;
	// 開箱人員姓名
	private String box_opener_name;
	// 開箱人員姓名
	private Time entry_time;
	// 開箱人員姓名
	private Time leave_time;
	// 夥同進出人數(含開箱者)
	private Integer attendant_count;
	// 陪同人員1:身分證字號
	private String attendant_id_1;
	// 陪同人員1:姓名
	private String attendant_name_1;
	// 陪同人員1:生日
	private Date attendant_birth_1;	
	// 陪同人員2:身分證字號
	private String attendant_id_2;
	// 陪同人員2:姓名
	private String attendant_name_2;
	// 陪同人員2:生日
	private Date attendant_birth_2;	
	// 陪同人員3:身分證字號
	private String attendant_id_3;
	// 陪同人員3:姓名
	private String attendant_name_3;
	// 陪同人員3:生日
	private Date attendant_birth_3;	
	// 陪同人員4:身分證字號
	private String attendant_id_4;
	// 陪同人員4:姓名
	private String attendant_name_4;
	// 陪同人員4:生日
	private Date attendant_birth_4;
	// 陪同人員5:身分證字號
	private String attendant_id_5;
	// 陪同人員5:姓名
	private String attendant_name_5;
	// 陪同人員5:生日
	private Date attendant_birth_5;
	// 備註
	private String box_open_description;
	// 錯誤註記
	private String error_mark = "";// 預設無錯誤
	// 上傳批號(測試用)  // TODO V4
	private String upload_no;
	
	// Constructor
	public ETL_Bean_SCUSTBOXOPEN_Data(ETL_Tool_ParseFileName pfn) {
		
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
	
	public Date getBox_open_date() {
		return box_open_date;
	}

	public void setBox_open_date(Date box_open_date) {
		this.box_open_date = box_open_date;
	}
	
	public String getBox_opener_id() {
		return box_opener_id;
	}

	public void setBox_opener_id(String box_opener_id) {
		this.box_opener_id = box_opener_id;
	}
	
	public String getBox_opener_name() {
		return box_opener_name;
	}

	public void setBox_opener_name(String box_opener_name) {
		this.box_opener_name = box_opener_name;
	}
	
	public Time getEntry_time() {
		return entry_time;
	}

	public void setEntry_time(Time entry_time) {
		this.entry_time = entry_time;
	}
	
	public Time getLeave_time() {
		return leave_time;
	}

	public void setLeave_time(Time leave_time) {
		this.leave_time = leave_time;
	}
	
	public Integer getAttendant_count() {
		return attendant_count;
	}

	public void setAttendant_count(Integer attendant_count) {
		this.attendant_count = attendant_count;
	}
	
	//陪同人員1
	public String getAttendant_id_1() {
		return attendant_id_1;
	}

	public void setAttendant_id_1(String attendant_id_1) {
		this.attendant_id_1 = attendant_id_1;
	}
	
	public String getAttendant_name_1() {
		return attendant_name_1;
	}

	public void setAttendant_name_1(String attendant_name_1) {
		this.attendant_name_1 = attendant_name_1;
	}
	
	public Date getAttendant_birth_1() {
		return attendant_birth_1;
	}

	public void setAttendant_birth_1(Date attendant_birth_1) {
		this.attendant_birth_1 = attendant_birth_1;
	}
	
	//陪同人員2
	public String getAttendant_id_2() {
		return attendant_id_2;
	}

	public void setAttendant_id_2(String attendant_id_2) {
		this.attendant_id_2 = attendant_id_2;
	}
	
	public String getAttendant_name_2() {
		return attendant_name_2;
	}

	public void setAttendant_name_2(String attendant_name_2) {
		this.attendant_name_2 = attendant_name_2;
	}
	
	public Date getAttendant_birth_2() {
		return attendant_birth_2;
	}

	public void setAttendant_birth_2(Date attendant_birth_2) {
		this.attendant_birth_2 = attendant_birth_2;
	}
	
	//陪同人員3
	public String getAttendant_id_3() {
		return attendant_id_3;
	}

	public void setAttendant_id_3(String attendant_id_3) {
		this.attendant_id_3 = attendant_id_3;
	}
	
	public String getAttendant_name_3() {
		return attendant_name_3;
	}

	public void setAttendant_name_3(String attendant_name_3) {
		this.attendant_name_3 = attendant_name_3;
	}
	
	public Date getAttendant_birth_3() {
		return attendant_birth_3;
	}

	public void setAttendant_birth_3(Date attendant_birth_3) {
		this.attendant_birth_3 = attendant_birth_3;
	}

	//陪同人員4
	public String getAttendant_id_4() {
		return attendant_id_4;
	}

	public void setAttendant_id_4(String attendant_id_4) {
		this.attendant_id_4 = attendant_id_4;
	}
	
	public String getAttendant_name_4() {
		return attendant_name_4;
	}

	public void setAttendant_name_4(String attendant_name_4) {
		this.attendant_name_4 = attendant_name_4;
	}
	
	public Date getAttendant_birth_4() {
		return attendant_birth_4;
	}

	public void setAttendant_birth_4(Date attendant_birth_4) {
		this.attendant_birth_4 = attendant_birth_4;
	}
	
	//陪同人員5
	public String getAttendant_id_5() {
		return attendant_id_5;
	}

	public void setAttendant_id_5(String attendant_id_5) {
		this.attendant_id_5 = attendant_id_5;
	}
	
	public String getAttendant_name_5() {
		return attendant_name_5;
	}

	public void setAttendant_name_5(String attendant_name_5) {
		this.attendant_name_5 = attendant_name_5;
	}
	
	public Date getAttendant_birth_5() {
		return attendant_birth_5;
	}

	public void setAttendant_birth_5(Date attendant_birth_5) {
		this.attendant_birth_5 = attendant_birth_5;
	}

	public String getBox_open_description() {
		return box_open_description;
	}

	public void setBox_open_description(String box_open_description) {
		this.box_open_description = box_open_description;
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
