package Bean;

import Tool.ETL_Tool_DM_ParseFileName;
import Tool.ETL_Tool_ParseFileName;

public class ETL_Bean_ErrorLog_Data {
	// ETL Error Log Data
	
	private String BATCH_NO; // 批次編號
	private String CENTRAL_NO; // 報送單位
	private java.util.Date RECORD_DATE; // 檔案日期
	private String FILE_TYPE; // 檔名業務別
	private String FILE_NAME; // 處理類型檔案名稱
	private String UPLOAD_NO; // 上傳批號
	private String STEP_TYPE; // 步驟
	private String ROW_COUNT; // 行數
	private String FIELD_NAME; // 欄位中文名稱
	private String ERROR_DESCRIPTION; // 錯誤描述
	private String SRC_FILE; // 來源檔案(檔案全名)
	// Error_Log錯誤回覆機制用
	private String ERROR_TYPE = "Error"; // 錯誤類別(預設Error)
	private String DOMAIN_ID; // 本會代號
	private String PARTY_NUMBER; // 客戶統編
	private String TRANSACTION_ID; // 交易序號
	private String ACCOUNT_ID; // 帳號
	private String LOAN_MASTER_NUMBER; // 批覆書編號
	private String LOAN_DETAIL_NUMBER; // 額度編號
	private String COLLATERAL_ID; // 擔保品編號
	private String DEFAULT_VALUE; // 欄位預設值
	private java.util.Date MODIFY_DATE; // 資料更正日期
	private java.sql.Timestamp CHECK_EXCUTE_DATETIME; // 執行日期時間
	private String SRC_DATA; // 原始資料
	
	
	/**
	 * 
	 * @param CENTRAL_NO 報送單位
	 * @param RECORD_DATE 檔案日期
	 * @param FILE_TYPE 檔名業務別
	 * @param FILE_NAME 處理類型檔案名稱
	 * @param UPLOAD_NO 上傳批號
	 * @param STEP_TYPE 步驟
	 * @param ROW_COUNT 行數
	 * @param FIELD_NAME 欄位中文名稱
	 * @param ERROR_DESCRIPTION 錯誤描述
	 * @param SRC_FILE 來源檔案(檔案全名)
	 */
	// ETL_P_ErrorLog_Data's Constructor 第一版, 直接輸入參數
	public ETL_Bean_ErrorLog_Data(String BATCH_NO, String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE, String FILE_NAME, String UPLOAD_NO,
			String STEP_TYPE, String ROW_COUNT, String FIELD_NAME, String ERROR_DESCRIPTION, String SRC_FILE) {
		
		this.BATCH_NO = BATCH_NO;
		this.CENTRAL_NO = CENTRAL_NO;
		this.RECORD_DATE = RECORD_DATE;
		this.FILE_TYPE = FILE_TYPE;
		this.FILE_NAME = FILE_NAME;
		this.UPLOAD_NO = UPLOAD_NO;
		this.STEP_TYPE = STEP_TYPE;
		this.ROW_COUNT = ROW_COUNT;
		this.FIELD_NAME = FIELD_NAME;
		this.ERROR_DESCRIPTION = ERROR_DESCRIPTION;
		this.SRC_FILE = SRC_FILE;
	}
	
	/**
	 *
	 * @param pfn 報送單位, 檔案日期, 檔名業務別, 處理類型檔案名稱, 來源檔案(檔案全名)
	 * @param UPLOAD_NO 上傳批號
	 * @param STEP_TYPE 步驟
	 * @param ROW_COUNT 行數
	 * @param FIELD_NAME 欄位中文名稱
	 * @param ERROR_DESCRIPTION 錯誤描述
	 */
	// ETL_P_ErrorLog_Data's Constructor 第二版, 加入解析檔名物件
	public ETL_Bean_ErrorLog_Data(ETL_Tool_ParseFileName pfn, String UPLOAD_NO, String STEP_TYPE, String ROW_COUNT, String FIELD_NAME, String ERROR_DESCRIPTION) {
		this.BATCH_NO = pfn.getBatch_no();
		this.CENTRAL_NO = pfn.getCentral_No();
		this.RECORD_DATE = pfn.getRecord_Date();
		this.FILE_TYPE = pfn.getFile_Type();
		this.FILE_NAME = pfn.getFile_Name();
		this.UPLOAD_NO = UPLOAD_NO;
		this.STEP_TYPE = STEP_TYPE;
		this.ROW_COUNT = ROW_COUNT;
		this.FIELD_NAME = FIELD_NAME;
		this.ERROR_DESCRIPTION = ERROR_DESCRIPTION;
		this.SRC_FILE = pfn.getFileName();
	}
	
	// DM版本
	public ETL_Bean_ErrorLog_Data(ETL_Tool_DM_ParseFileName pfn, String UPLOAD_NO, String STEP_TYPE, String ROW_COUNT, String FIELD_NAME, String ERROR_DESCRIPTION) {
		this.BATCH_NO = pfn.getBatch_no();
		this.CENTRAL_NO = pfn.getCentral_no();
		this.RECORD_DATE = pfn.getRecord_date();
		this.FILE_NAME = pfn.getFile_name();
		this.UPLOAD_NO = UPLOAD_NO;
		this.STEP_TYPE = STEP_TYPE;
		this.ROW_COUNT = ROW_COUNT;
		this.FIELD_NAME = FIELD_NAME;
		this.ERROR_DESCRIPTION = ERROR_DESCRIPTION;
		this.SRC_FILE = pfn.getFileName();
	}
	
//	// Error型態改為Warning
//	public void setWarning() {
//		this.ERROR_TYPE = "Warning";
//	}

	public String getBATCH_NO() {
		return BATCH_NO;
	}

	public void setBATCH_NO(String bATCH_NO) {
		BATCH_NO = bATCH_NO;
	}

	public String getCENTRAL_NO() {
		return CENTRAL_NO;
	}

	public void setCENTRAL_NO(String cENTRAL_NO) {
		CENTRAL_NO = cENTRAL_NO;
	}

	public java.util.Date getRECORD_DATE() {
		return RECORD_DATE;
	}

	public void setRECORD_DATE(java.util.Date rECORD_DATE) {
		RECORD_DATE = rECORD_DATE;
	}

	public String getFILE_TYPE() {
		return FILE_TYPE;
	}

	public void setFILE_TYPE(String fILE_TYPE) {
		FILE_TYPE = fILE_TYPE;
	}

	public String getFILE_NAME() {
		return FILE_NAME;
	}

	public void setFILE_NAME(String fILE_NAME) {
		FILE_NAME = fILE_NAME;
	}

	public String getUPLOAD_NO() {
		return UPLOAD_NO;
	}

	public void setUPLOAD_NO(String uPLOAD_NO) {
		UPLOAD_NO = uPLOAD_NO;
	}

	public String getSTEP_TYPE() {
		return STEP_TYPE;
	}

	public void setSTEP_TYPE(String sTEP_TYPE) {
		STEP_TYPE = sTEP_TYPE;
	}

	public String getROW_COUNT() {
		return ROW_COUNT;
	}

	public void setROW_COUNT(String rOW_COUNT) {
		ROW_COUNT = rOW_COUNT;
	}

	public String getFIELD_NAME() {
		return FIELD_NAME;
	}

	public void setFIELD_NAME(String fIELD_NAME) {
		FIELD_NAME = fIELD_NAME;
	}

	public String getERROR_DESCRIPTION() {
		return ERROR_DESCRIPTION;
	}

	public void setERROR_DESCRIPTION(String eRROR_DESCRIPTION) {
		ERROR_DESCRIPTION = eRROR_DESCRIPTION;
	}

	public String getSRC_FILE() {
		return SRC_FILE;
	}

	public void setSRC_FILE(String sRC_FILE) {
		SRC_FILE = sRC_FILE;
	}

	public String getERROR_TYPE() {
		return ERROR_TYPE;
	}

	public void setERROR_TYPE(String eRROR_TYPE) {
		ERROR_TYPE = eRROR_TYPE;
	}

	public String getDOMAIN_ID() {
		return DOMAIN_ID;
	}

	public void setDOMAIN_ID(String dOMAIN_ID) {
		DOMAIN_ID = dOMAIN_ID;
	}

	public String getPARTY_NUMBER() {
		return PARTY_NUMBER;
	}

	public void setPARTY_NUMBER(String pARTY_NUMBER) {
		PARTY_NUMBER = pARTY_NUMBER;
	}

	public String getTRANSACTION_ID() {
		return TRANSACTION_ID;
	}

	public void setTRANSACTION_ID(String tRANSACTION_ID) {
		TRANSACTION_ID = tRANSACTION_ID;
	}

	public String getACCOUNT_ID() {
		return ACCOUNT_ID;
	}

	public void setACCOUNT_ID(String aCCOUNT_ID) {
		ACCOUNT_ID = aCCOUNT_ID;
	}

	public String getLOAN_MASTER_NUMBER() {
		return LOAN_MASTER_NUMBER;
	}

	public void setLOAN_MASTER_NUMBER(String lOAN_MASTER_NUMBER) {
		LOAN_MASTER_NUMBER = lOAN_MASTER_NUMBER;
	}

	public String getLOAN_DETAIL_NUMBER() {
		return LOAN_DETAIL_NUMBER;
	}

	public void setLOAN_DETAIL_NUMBER(String lOAN_DETAIL_NUMBER) {
		LOAN_DETAIL_NUMBER = lOAN_DETAIL_NUMBER;
	}

	public String getCOLLATERAL_ID() {
		return COLLATERAL_ID;
	}

	public void setCOLLATERAL_ID(String cOLLATERAL_ID) {
		COLLATERAL_ID = cOLLATERAL_ID;
	}

	public String getDEFAULT_VALUE() {
		return DEFAULT_VALUE;
	}

	public void setDEFAULT_VALUE(String dEFAULT_VALUE) {
		// 寫入預設值, 預設為Warning
		this.ERROR_TYPE = "Warning";
		DEFAULT_VALUE = dEFAULT_VALUE;
	}

	public java.util.Date getMODIFY_DATE() {
		return MODIFY_DATE;
	}

	public void setMODIFY_DATE(java.util.Date mODIFY_DATE) {
		MODIFY_DATE = mODIFY_DATE;
	}

	public java.sql.Timestamp getCHECK_EXCUTE_DATETIME() {
		return CHECK_EXCUTE_DATETIME;
	}

	public void setCHECK_EXCUTE_DATETIME(java.sql.Timestamp cHECK_EXCUTE_DATETIME) {
		CHECK_EXCUTE_DATETIME = cHECK_EXCUTE_DATETIME;
	}

	public String getSRC_DATA() {
		return SRC_DATA;
	}

	public void setSRC_DATA(String sRC_DATA) {
		SRC_DATA = sRC_DATA;
	}
		
}
