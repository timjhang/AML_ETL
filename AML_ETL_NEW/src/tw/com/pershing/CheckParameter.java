package tw.com.pershing;

import java.io.File;

import Tool.ETL_Tool_FormatCheck;

public class CheckParameter {
	
	// 檢查E,T系列程式傳入參數,  若有錯誤 throw相關錯誤訊息Exception
	public void check(String filePath, String batch_no, String exc_central_no, 
			String exc_record_dateStr, String upload_no/*, String program_no*/) throws Exception {
		
		if (ETL_Tool_FormatCheck.isEmpty(filePath)) {
			throw new Exception("傳入檔案路徑為空值！");
		}
		File file = new File(filePath);
		if (!file.isDirectory()) {
			throw new Exception("傳入路徑非資料夾路徑！");
		}
		
		if (ETL_Tool_FormatCheck.isEmpty(batch_no)) {
			throw new Exception("批次編號為空值！");
		}
		if (batch_no.getBytes().length > 8) {
			throw new Exception("批次編號長度超過8！");
		}
		
		if (ETL_Tool_FormatCheck.isEmpty(exc_central_no)) {
			throw new Exception("報送單位代碼為空值！");
		}
		if (exc_central_no.getBytes().length > 7) {
			throw new Exception("報送單位代碼長度超過7！");
		}
		
		// 資料日期轉換型態
		if (ETL_Tool_FormatCheck.isEmpty(exc_record_dateStr)) {
			throw new Exception("資料日期為空值！");
		}
		// 會傳出Exception, 資料日期轉換有錯誤不適合往下, 跳出繼續往下
		if (ETL_Tool_FormatCheck.checkDate(exc_record_dateStr)) {
			// empty
		} else {
			throw new Exception("資料日期格式錯誤！");
		}
		
		if (ETL_Tool_FormatCheck.isEmpty(upload_no)) {
			throw new Exception("上傳批號為空值！");
		}
		if (upload_no.getBytes().length > 3) {
			throw new Exception("上傳批號長度超過3！");
		}
		
//		if (ETL_Tool_FormatCheck.isEmpty(program_no)) {
//			throw new Exception("程式代號為空值！");
//		}
//		if (program_no.getBytes().length > 40) {
//			throw new Exception("程式代號長度超過40！");
//		}
		
	}

}
