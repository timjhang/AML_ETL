package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_TRANSACTION_Data;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_EData_Filter;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.ETL_Q_ColumnCheckCodes;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FileByteUtil;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_E_TRANSACTION extends Extract {

	public ETL_E_TRANSACTION() {
		
	}

	public ETL_E_TRANSACTION(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) {
		super(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
	}

	@Override
	public void read_File() {
		try {
			read_Transaction_File(this.filePath, this.fileTypeName, this.batch_no, this.exc_central_no,
					this.exc_record_date, this.upload_no, this.program_no);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = {
			{ "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "currency_code", "COMM_CURRENCY_CODE" }, // 交易幣別
			{ "amt_sign", "TRANSACTION_AMT_SIGN" }, // 交易金額正負號
			{ "direction", "TRANSACTION_DIRECTION" }, // 存提區分
			{ "transaction_type", "COMM_TRANSACTION_TYPE" }, // 交易類別
			{ "channel_type", "TRANSACTION_CHANNEL_TYPE" }, // 交易管道
			{ "ec_flag", "TRANSACTION_EC_FLAG" } // 更正記號
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;
	
	// insert errorLog fail Count  // TODO V6_2
	private int oneFileInsertErrorCount = 0;

	// Data儲存List
	private List<ETL_Bean_TRANSACTION_Data> dataList = new ArrayList<ETL_Bean_TRANSACTION_Data>();

	// class生成時, 取得所有檢核用子map, 置入母map內 // TODO V6_2
//	{
//		try {
//
//			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(checkMapArray);
//
//		} catch (Exception ex) {
//			checkMaps = null;
//			System.out.println("ETL_E_TRANSACTION 抓取checkMaps資料有誤!");
//			ex.printStackTrace();
//		}
//	};

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	// filePath 讀檔路徑
	// fileTypeName 讀檔業務別
	// batch_no 批次編號
	// exc_central_no 批次執行_報送單位
	// exc_record_date 批次執行_檔案日期
	// upload_no 上傳批號
	// program_no 程式代號
	public void read_Transaction_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		// TODO  V6_2 start
		// 取得所有檢核用子map, 置入母map內
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(exc_record_date, exc_central_no, checkMapArray);
		
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_TRANSACTION 抓取checkMaps資料有誤!"); // TODO  V6_2
			ex.printStackTrace();
		}
		// TODO  V6_2 end
		
		System.out.println("#######Extrace - ETL_E_TRANSACTION - Start");

		try {
			// 批次不重複執行
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_TRANSACTION - 不重複執行\n" + inforMation);
				System.out.println("#######Extrace - ETL_E_TRANSACTION - End");

				return;
			}
			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理TRANSACTION錯誤計數
			int[] detail_ErrorCount = new int[1];
			detail_ErrorCount[0] = 0;

			// 程式執行錯誤訊息
			String[] processErrMsg = new String[1];
			processErrMsg[0] = "";

			// 取得目標檔案File
			List<File> fileList = ETL_Tool_FileReader.getTargetFileList(filePath, fileTypeName);

			System.out.println("共有檔案 " + fileList.size() + " 個！");
			System.out.println("===============");
			for (int i = 0; i < fileList.size(); i++) {
				System.out.println(fileList.get(i).getName());
			}
			System.out.println("===============");

			
			// 線程List
			List<ExtractTransaction> extractTransactionList = new ArrayList<ExtractTransaction>();
			
			// 處理檔案加入線程
			for (int i = 0; i < fileList.size(); i++) {
				extractTransactionList.add(
						new ExtractTransaction(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no,
								detail_ErrorCount, processErrMsg, fileList.get(i), checkMaps));
			}
			
			System.out.println("fileList.size() = " + fileList.size());
//			ExecutorService executor = Executors.newFixedThreadPool(fileList.size());
			ExecutorService executor = Executors.newFixedThreadPool(2);
			
			for (ExtractTransaction extractTransaction : extractTransactionList) {
				executor.execute(extractTransaction);
			}
			
			executor.shutdown();

			while (!executor.isTerminated()) {
			}
			
			System.out.println("線程池已經關閉");
			
			
			// TODO V6_2 Start
			// 過濾軌跡資料
			try {
				
				ETL_P_EData_Filter.E_Datas_Filter("filter_Transaction_Temp_Temp", // TODO v6_2
						batch_no, exc_central_no, exc_record_date, upload_no, program_no);
				
			} catch (Exception ex) {
				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
						"E", "0", "ETL_E_TRANSACTION程式處理", ex.getMessage(), null); // TODO v6_2
				processErrMsg[0] = processErrMsg[0] + ex.getMessage() + "\n";
				
				ex.printStackTrace();
			}
			// TODO V6_2 End

			// 執行結果
			String detail_exe_result;
			// 執行結果說明
			String detail_exe_result_description;

			if (fileList.size() == 0) {
				detail_exe_result = "S";
				detail_exe_result_description = "缺檔案類型：" + fileTypeName + " 檔案";

				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
						"0", "ETL_E_TRANSACTION程式處理", detail_exe_result_description, null);

			} else if (!"".equals(processErrMsg[0])) {
				detail_exe_result = "S";
				detail_exe_result_description = processErrMsg[0];
			} else if (detail_ErrorCount[0] == 0) {
				detail_exe_result = "Y";
				detail_exe_result_description = "檔案轉入檢核無錯誤";
			} else {
				detail_exe_result = "N";
				detail_exe_result_description = "錯誤資料筆數: " + detail_ErrorCount[0];
			}

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", detail_exe_result, detail_exe_result_description, new Date());

		} catch (Exception ex) {
			// 寫入Error_Log
			ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
					"0", "ETL_E_TRANSACTION程式處理", ex.getMessage(), null);

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}
		System.out.println("#######Extrace - ETL_E_TRANSACTION - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_TRANSACTION_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Transaction_Datas();
		}
	}

	// 將TRANSACTION資料寫入資料庫
	private void insert_Transaction_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_TRANSACTION - insert_Transaction_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫TRANSACTION寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_TRANSACTION_TEMP(?,?)}");// TODO V6_2
		// DB2 type - TRANSACTION
		insertAdapter.setCreateStructTypeName("T_TRANSACTION");
		// DB2 array type - TRANSACTION
		insertAdapter.setCreateArrayTypesName("A_TRANSACTION");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.dataList, insertAdapter); // TODO V6_2
		int errorCount = insertAdapter.getErrorCount(); // TODO V6_2
		
		if (isSuccess) {
			System.out.println("insert_Transaction_Datas 寫入 " + this.dataList.size() + "(-" + errorCount + ")筆資料!"); // TODO V6_2
			this.oneFileInsertErrorCount = this.oneFileInsertErrorCount + errorCount; // TODO V6_2
		} else {
			throw new Exception("insert_Transaction_Datas 發生錯誤");
		}
		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}

	/**
	 * 檢測在特定欄位等於特定值時，會觸發另外一個欄位為必填時的規則
	 * 
	 * @param source
	 *            特定欄位
	 * @param now
	 *            被觸發的欄位
	 * @param compareVal
	 *            特定值
	 * @return true 符合規則 / false 不符合規則
	 */
	private static boolean specialRequired(String source, String now, String compareVal) {
		return (!ETL_Tool_FormatCheck.isEmpty(source) && compareVal.equals(source.trim())
				&& ETL_Tool_FormatCheck.isEmpty(now)) ? false : true;
	}

	public static void main(String[] argv) throws Exception {

		// 讀取測試資料，並列出明細錄欄位
		// Charset charset = Charset.forName("Big5");
		// List<String> lines = Files.readAllLines(
		// Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\600_R_TRANSACTION_20171206.TXT"),
		// charset);
		// // 難字轉換工具
		// ETL_Tool_Big5_To_UTF8 wordsXTool = new
		// ETL_Tool_Big5_To_UTF8(ETL_Profile.DifficultWords_Lists_Path);
		// // 難字轉換map
		// Map<String, Map<String, String>> difficultWordMaps=
		// wordsXTool.getDifficultWordMaps("600");

		// if (lines.size() > 2) {
		//
		// lines.remove(0);
		// lines.remove(lines.size() - 1);
		//
		// System.out.println(
		// "============================================================================================");
		// for (String line : lines) {
		// byte[] tmp = line.getBytes(charset);
		// System.out.println("第" + (lines.indexOf(line) + 1) + "行");
		// System.out.println("位元組長度: " + tmp.length);
		// System.out.println("區別碼X(01): " + new String(Arrays.copyOfRange(tmp,
		// 0, 1), "Big5"));
		// System.out.println("本會代號X(07): " + new String(Arrays.copyOfRange(tmp,
		// 1, 8), "Big5"));
		// System.out.println("客戶統編X(11): " + new String(Arrays.copyOfRange(tmp,
		// 8, 19), "Big5"));
		// System.out.println("帳號X(30): " + new String(Arrays.copyOfRange(tmp,
		// 19, 49), "Big5"));
		// System.out.println("主機交易序號X(20): " + new
		// String(Arrays.copyOfRange(tmp, 49, 69), "Big5"));
		// System.out.println("作帳日X(08): " + new String(Arrays.copyOfRange(tmp,
		// 69, 77), "Big5"));
		// System.out.println("實際交易時間X(14): " + new
		// String(Arrays.copyOfRange(tmp, 77, 91), "Big5"));
		// System.out.println("交易幣別X(03): " + new String(Arrays.copyOfRange(tmp,
		// 91, 94), "Big5"));
		// System.out.println("交易金額正負號X(01): " + new
		// String(Arrays.copyOfRange(tmp, 94, 95), "Big5"));
		// System.out.println("交易金額9(12)V99 : " + new
		// String(Arrays.copyOfRange(tmp, 95, 109), "Big5"));
		// System.out.println("存提區分X(01): " + new String(Arrays.copyOfRange(tmp,
		// 109, 110), "Big5"));
		// System.out.println("交易類別X(04): " + new String(Arrays.copyOfRange(tmp,
		// 110, 114), "Big5"));
		// System.out.println("交易管道X(03): " + new String(Arrays.copyOfRange(tmp,
		// 114, 117), "Big5"));
		// System.out.println("交易代號X(10): " + new String(Arrays.copyOfRange(tmp,
		// 117, 127), "Big5"));
		// System.out.println("原交易代號X(10): " + new
		// String(Arrays.copyOfRange(tmp, 127, 137), "Big5"));
		// System.out.println("交易摘要X(05): " + new String(Arrays.copyOfRange(tmp,
		// 137, 142), "Big5"));
		// System.out.println("備註X(80): " + new String(Arrays.copyOfRange(tmp,
		// 142, 222), "Big5"));
		// System.out.println("操作行X(07): " + new String(Arrays.copyOfRange(tmp,
		// 222, 229), "Big5"));

		// StringBuffer stringBuffer = new StringBuffer();
		// for(byte b:Arrays.copyOfRange(tmp, 249, 329)){
		// stringBuffer.append("[").append(b).append("]");
		// }
		// System.out.println(stringBuffer.toString());

		// System.out.println("操作櫃員代號或姓名X(20): "
		// +wordsXTool.format(Arrays.copyOfRange(tmp, 229, 249),
		// difficultWordMaps));
		// System.out.println("匯款人姓名X(80): "
		// +wordsXTool.format(Arrays.copyOfRange(tmp, 249, 329),
		// difficultWordMaps));
		// System.out.println("受款人姓名X(80): "
		// +wordsXTool.format(Arrays.copyOfRange(tmp, 329, 409),
		// difficultWordMaps));

		// System.out.println("操作櫃員代號或姓名X(20): " + new
		// String(Arrays.copyOfRange(tmp, 229, 249), "Big5"));
		// System.out.println("匯款人姓名X(80): " + new
		// String(Arrays.copyOfRange(tmp, 249, 329), "Big5"));
		// System.out.println("受款人姓名X(80): " + new
		// String(Arrays.copyOfRange(tmp, 329, 409), "Big5"));

		// System.out.println("受款人銀行X(08): " + new
		// String(Arrays.copyOfRange(tmp, 409, 417), "Big5"));
		// System.out.println("受款人帳號 X(50): " + new
		// String(Arrays.copyOfRange(tmp, 417, 467), "Big5"));
		// System.out.println("還款本金9(12)V99: " + new
		// String(Arrays.copyOfRange(tmp, 467, 481), "Big5"));
		// System.out.println("更正記號X(01): " + new String(Arrays.copyOfRange(tmp,
		// 481, 482), "Big5"));
		// System.out.println("申報國別X(02): " + new String(Arrays.copyOfRange(tmp,
		// 482, 484), "Big5"));
		// System.out.println(
		// "============================================================================================");
		// }
		// }

		// 讀取測試資料，並運行程式
		ETL_E_TRANSACTION one = new ETL_E_TRANSACTION();
		String filePath = "C:\\Users\\10404003\\Desktop\\農金\\2018\\180612\\test";
		String fileTypeName = "TRANSACTION";

		long time1, time2;
		time1 = System.currentTimeMillis();

//		byte[]  file_018= Files.readAllBytes(Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\018_L_TRANSACTION_20180131_______ - 複製.TXT"));
//		byte[] file_600 = Files.readAllBytes(Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\600_R_TRANSACTION_20180221.TXT"));
//		byte[] file_928_old = Files.readAllBytes(Paths.get("D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\928_K_TRANSACTION_20180105.TXT"));
//		System.out.println("file_600: "+file_600.length);
//		System.out.println("file_018: "+file_018.length);
//		System.out.println("file_928_old: "+file_928_old.length);
		one.read_Transaction_File(filePath, fileTypeName, "Tim00051", "018",
				new SimpleDateFormat("yyyyMMdd").parse("20180611"), "008", "ETL_E_TRANSACTION");

		time2 = System.currentTimeMillis();
		System.out.println("花了：" + (time2 - time1) + "豪秒");

	}

}
