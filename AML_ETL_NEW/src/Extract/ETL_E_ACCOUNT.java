package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ACCOUNT_Data;
import Bean.ETL_Bean_ErrorLog_Data;
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

public class ETL_E_ACCOUNT extends Extract {

	public ETL_E_ACCOUNT() {

	}

	public ETL_E_ACCOUNT(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) {
		super(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
	}

	@Override
	public void read_File() {
		try {
			read_Account_File(this.filePath, this.fileTypeName, this.batch_no, this.exc_central_no,
					this.exc_record_date, this.upload_no, this.program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "change_code", "ACCOUNT_CHANGE_CODE" }, // 異動代號
			{ "account_type_code", "ACCOUNT_ACCOUNT_TYPE_CODE" }, // 帳戶類別
			{ "account_type_code018", "ACCOUNT_ACCOUNT_TYPE_CODE018" }, // 帳戶類別
			{ "branch_code", "COMM_BRANCH_CODE" }, // 帳戶行
			{ "property_code", "ACCOUNT_PROPERTY_CODE" }, // 連結服務
			{ "currency_code", "COMM_CURRENCY_CODE" }, // 幣別
			{ "status_code", "ACCOUNT_STATUS_CODE" }, // 帳戶狀態
			{ "account_opening_channel", "ACCOUNT_ACCOUNT_OPENING_CHANNEL" }, // 開戶管道
			{ "balance_acct_currency_sign", "ACCOUNT_BALANCE_ACCT_CURRENCY_SIGN" }, // 帳戶餘額正負號
			{ "balance_last_month_avg_sign", "ACCOUNT_BALANCE_LAST_MONTH_AVG_SIGN" }, // 過去一個月平均餘額正負號
			{ "caution_note", "ACCOUNT_CAUTION_NOTE" } // 警示註記
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// insert errorLog fail Count
	private int oneFileInsertErrorCount = 0;

	// Data儲存List
	private List<ETL_Bean_ACCOUNT_Data> dataList = new ArrayList<ETL_Bean_ACCOUNT_Data>();

	/**
	 * 讀取檔案 : 根據 讀檔業務別, 開啟讀檔路徑中符合檔案
	 * 
	 * @param filePath 讀檔路徑
	 * @param fileTypeName 讀檔業務別
	 * @param batch_no 批次編號
	 * @param exc_central_no 批次執行_報送單位
	 * @param exc_record_date 批次執行_檔案日期
	 * @param upload_no 上傳批號
	 * @param program_no 程式代號
	 * @throws Exception
	 */
	public void read_Account_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		// 取得所有檢核用子map, 置入母map內
		try {
			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(exc_record_date, exc_central_no, checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_ACCOUNT 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_ACCOUNT - Start");

		try {
			// 批次不重複執行
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_ACCOUNT - 不重複執行\n" + inforMation);
				System.out.println("#######Extrace - ETL_E_ACCOUNT - End");

				return;
			}

			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理ACCOUNT錯誤計數
			int detail_ErrorCount = 0;

			// 程式執行錯誤訊息
			String processErrMsg = "";

			// 取得目標檔案File
			List<File> fileList = ETL_Tool_FileReader.getTargetFileList(filePath, fileTypeName);

			System.out.println("共有檔案 " + fileList.size() + " 個！");
			System.out.println("===============");
			for (int i = 0; i < fileList.size(); i++) {
				System.out.println(fileList.get(i).getName());
			}
			System.out.println("===============");

			// 進行檔案處理
			for (int i = 0; i < fileList.size(); i++) {
				// 取得檔案
				File parseFile = fileList.get(i);

				ETL_Tool_FileByteUtil fileByteUtil = new ETL_Tool_FileByteUtil(parseFile.getAbsolutePath(),
						ETL_E_ACCOUNT.class);

				// 檔名
				String fileName = parseFile.getName();

				// 讀檔檔名英文字轉大寫比較
				if (!ETL_Tool_FormatCheck.isEmpty(fileName))
					fileName = fileName.toUpperCase();

				Date parseStartDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " Start " + parseStartDate);

				// 解析fileName物件
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName);

				// 設定批次編號
				pfn.setBatch_no(batch_no);

				// 設定上傳批號
				pfn.setUpload_no(upload_no);

				// 報送單位非預期, 不進行解析
				if (exc_central_no == null || "".equals(exc_central_no.trim())) {
					System.out.println("## ETL_E_ACCOUNT - read_Account_File - 控制程式無提供報送單位，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供報送單位，不進行解析！\n";
					continue;
				} else if (!exc_central_no.trim().equals(pfn.getCentral_No().trim())) {
					System.out.println("##" + pfn.getFileName() + " 處理報送單位非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理報送單位非預期，不進行解析！\n";
					continue;
				}

				// 業務別非預期, 不進行解析
				if (pfn.getFile_Type() == null || "".equals(pfn.getFile_Type().trim())
						|| !checkMaps.get("comm_file_type").containsKey(pfn.getFile_Type().trim())) {

					System.out.println("##" + pfn.getFileName() + " 處理業務別非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理業務別非預期，不進行解析！\n";
					continue;
				}

				// 資料日期非預期, 不進行解析
				if (exc_record_date == null) {
					System.out.println("## ETL_E_ACCOUNT - read_Account_File - 控制程式無提供資料日期，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供資料日期，不進行解析！\n";
					continue;
				} else if (!exc_record_date.equals(pfn.getRecord_Date())) {
					System.out.println("## " + pfn.getFileName() + " 處理資料日期非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理資料日期非預期，不進行解析！\n";
					continue;
				}

				// rowCount == 處理行數
				int rowCount = 1; // 從1開始
				// 成功計數
				int successCount = 0;
				// 警示計數(預設值)
				int warningCount = 0;
				// 失敗計數
				int failureCount = 0;
				// error_log data
				ETL_Bean_ErrorLog_Data errorLog_Data;
				// 紀錄是否第一次
				boolean isFirstTime = false;

				try {

					// 開始前ETL_FILE_Log寫入DB
					ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseStartDate, null, 0, 0, 0, 0,
							pfn.getFileName());

					// 嚴重錯誤訊息變數(讀檔)
					String fileFmtErrMsg = "";

					// ETL_字串處理Queue
					ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue(exc_central_no);
					// ETL_Error Log寫入輔助工具
					ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();

					int isFileOK = fileByteUtil.isFileOK(pfn, upload_no, parseFile.getAbsolutePath());

					boolean isFileFormatOK = isFileOK != 0 ? true : false;
					fileFmtErrMsg = isFileFormatOK ? "" : "區別碼錯誤";

					// 首錄檢查
					if (isFileFormatOK) {

						// 注入指定範圍筆數資料到QUEUE
						strQueue.setBytesList(fileByteUtil.getFilesBytes());

						// strQueue工具注入第一筆資料
						strQueue.setTargetString();

						// 檢查整行bytes數(1 + 7 + 8 + 97 = 113)
						if (strQueue.getTotalByteLength() != 113) {
							fileFmtErrMsg = "首錄位元數非預期113:" + strQueue.getTotalByteLength();
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別瑪檢核(1)，嚴重錯誤時,不進行迴圈並記錄錯誤訊息
						String typeCode = strQueue.popBytesString(1);
						if (!"1".equals(typeCode)) {
							fileFmtErrMsg = "首錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}

						// 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) {
							fileFmtErrMsg = "首錄報送單位代碼與檔名不符:" + central_no;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}

						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (ETL_Tool_FormatCheck.isEmpty(record_date)) {
							fileFmtErrMsg = "首錄檔案日期空值";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!record_date.equals(pfn.getRecord_Date_String())) {
							fileFmtErrMsg = "首錄檔案日期與檔名不符:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "首錄檔案日期格式錯誤:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 保留欄檢核(97)
						strQueue.popBytesString(97);

						rowCount++; // 處理行數 + 1
					}

					// 實際處理明細錄筆數
					int grandTotal = 0;

					// 明細錄檢查- 逐行讀取檔案
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						if (rowCount == 2)
							isFirstTime = true;
						
						// 以實際處理明細錄筆數為依據，只運行明細錄次數
						while (grandTotal < (isFileOK - 2)) {

							strQueue.setTargetString();


							// 生成一個Data
							ETL_Bean_ACCOUNT_Data data = new ETL_Bean_ACCOUNT_Data(pfn);
							data.setRow_count(rowCount);

							/*
							 * 整行bytes數檢核(1 + 7 + 11 + 1 + 30 + 7 + 2 + 1 + 3 +
							 * 1 + 1 + 8 + 8 + 1 + 14 + 1 + 14 + 2 = 113)
							 */
							if (strQueue.getTotalByteLength() != 113) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
												"行數bytes檢查", "非預期113:" + strQueue.getTotalByteLength()));

								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								failureCount++;
								rowCount++;
								grandTotal++;
								continue;
							}
							// 寫入ErrorList
							List<ETL_Bean_ErrorLog_Data> errorList = new ArrayList<ETL_Bean_ErrorLog_Data>();

							// 區別碼檢核 R X(01)*
							String typeCode = strQueue.popBytesString(1);
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"區別碼", "非預期:" + typeCode));
							}

							// 本會代號檢核 R X(07)*
							String domain_id = strQueue.popBytesString(7);
							data.setDomain_id(domain_id);

							if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"本會代號", "空值"));
							} else if (!checkMaps.get("domain_id").containsKey(domain_id.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"本會代號", "非預期:" + domain_id));
							}

							// 客戶統編檢核 R X(11)*
							String party_number = strQueue.popBytesString(11);
							data.setParty_number(party_number);

							if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"客戶統編", "空值"));
							}

							// 異動代號檢核 R X(01)*
							String change_code = strQueue.popBytesString(1);
							data.setChange_code(change_code);

							if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
								// 預設值 - U
								data.setChange_code("U");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "異動代號", "空值");
								errorLog_Data.setDEFAULT_VALUE("U");
								errorList.add(errorLog_Data);
							} else if (!checkMaps.get("change_code").containsKey(change_code.trim())) {
								// 預設值 - U
								data.setChange_code("U");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "異動代號", "非預期:" + change_code);
								errorLog_Data.setDEFAULT_VALUE("U");
								errorList.add(errorLog_Data);
							}

							// 帳號 R X(30)*
							String account_id = strQueue.popBytesString(30);
							data.setAccount_id(account_id);

							if (ETL_Tool_FormatCheck.isEmpty(account_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳號", "空值"));
							}

							// 帳戶行 R X(07)*
							String branch_code = strQueue.popBytesString(7);
							data.setBranch_code(branch_code);

							if (ETL_Tool_FormatCheck.isEmpty(branch_code)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳戶行", "空值"));
							} else if (!checkMaps.get("branch_code").containsKey(branch_code.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳戶行", "非預期:" + branch_code));
							}

							// 帳戶類別 R X(02)*
							String account_type_code = strQueue.popBytesString(2);
							data.setAccount_type_code(account_type_code);

							if (ETL_Tool_FormatCheck.isEmpty(account_type_code)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳戶類別", "空值"));
							} else if ("018".equals(exc_central_no)
									&& !checkMaps.get("account_type_code018").containsKey(account_type_code.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳戶類別018", "非預期:" + account_type_code));
							} else if (!"018".equals(exc_central_no)
									&& !checkMaps.get("account_type_code").containsKey(account_type_code.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳戶類別", "非預期:" + account_type_code));
							}

							// 連結服務 R X(01)*
							String property_code = strQueue.popBytesString(1);
							data.setProperty_code(property_code);

							if (ETL_Tool_FormatCheck.isEmpty(property_code)) {
								// 預設值 - 0
								data.setProperty_code("0");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "連結服務", "空值");
								errorLog_Data.setDEFAULT_VALUE("0");
								errorList.add(errorLog_Data);
							} else if (!checkMaps.get("property_code").containsKey(property_code.trim())) {
								// 預設值 - 0
								data.setProperty_code("0");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "連結服務", "非預期:" + property_code);
								errorLog_Data.setDEFAULT_VALUE("0");
								errorList.add(errorLog_Data);
							}

							// 幣別 R X(03)*
							String currency_code = strQueue.popBytesString(3);
							data.setCurrency_code(currency_code);

							if (ETL_Tool_FormatCheck.isEmpty(currency_code)) {
								// 預設值 - TWD
								data.setCurrency_code("TWD");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "幣別", "空值");
								errorLog_Data.setDEFAULT_VALUE("TWD");
								errorList.add(errorLog_Data);
							} else if (!checkMaps.get("currency_code").containsKey(currency_code.trim())) {
								// 預設值 - TWD
								data.setCurrency_code("TWD");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "幣別", "非預期:" + currency_code);
								errorLog_Data.setDEFAULT_VALUE("TWD");
								errorList.add(errorLog_Data);
							}

							// 帳戶狀態 R X(01)*
							String status_code = strQueue.popBytesString(1);
							data.setStatus_code(status_code);

							if (ETL_Tool_FormatCheck.isEmpty(status_code)) {
								// 預設值 - A
								data.setStatus_code("A");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "帳戶狀態", "空值");
								errorLog_Data.setDEFAULT_VALUE("A");
								errorList.add(errorLog_Data);
							} else if (!checkMaps.get("status_code").containsKey(status_code.trim())) {
								// 預設值 - A
								data.setStatus_code("A");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "帳戶狀態", "非預期:" + status_code);
								errorLog_Data.setDEFAULT_VALUE("A");
								errorList.add(errorLog_Data);
							}

							// 開戶管道 R X(01)*
							String account_opening_channel = strQueue.popBytesString(1);
							data.setAccount_opening_channel(account_opening_channel);

							if (ETL_Tool_FormatCheck.isEmpty(account_opening_channel)) {
								// 預設值 - P
								data.setAccount_opening_channel("P");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "開戶管道", "空值");
								errorLog_Data.setDEFAULT_VALUE("P");
								errorList.add(errorLog_Data);
							} else if (!checkMaps.get("account_opening_channel")
									.containsKey(account_opening_channel.trim())) {
								// 預設值 - P
								data.setAccount_opening_channel("P");
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "開戶管道", "非預期:" + account_opening_channel);
								errorLog_Data.setDEFAULT_VALUE("P");
								errorList.add(errorLog_Data);
							}

							// 開戶日期 R X(08)*
							String account_open_date = strQueue.popBytesString(8);
							data.setAccount_open_date(ETL_Tool_StringX.toUtilDate(account_open_date));

							if (ETL_Tool_FormatCheck.isEmpty(account_open_date)) {
								// 預設值 - 19000101
								data.setAccount_open_date(ETL_Tool_StringX.toUtilDate("00000000"));
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "開戶日期", "空值");
								errorLog_Data.setDEFAULT_VALUE("19000101");
								errorList.add(errorLog_Data);
							} else if (!ETL_Tool_FormatCheck.checkDate(account_open_date)) {
								// 預設值 - 19000101
								data.setAccount_open_date(ETL_Tool_StringX.toUtilDate("00000000"));
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "開戶日期", "格式錯誤:" + account_open_date);
								errorLog_Data.setDEFAULT_VALUE("19000101");
								errorList.add(errorLog_Data);
							}

							// 結清(銷戶)日期 O X(08)
							String account_close_date = strQueue.popBytesString(8);
							data.setAccount_close_date(ETL_Tool_StringX.toUtilDate(account_close_date));

							if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(account_close_date)) {
								if (!ETL_Tool_FormatCheck.checkDate(account_close_date)) {
									data.setError_mark("Y");
									errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "結清(銷戶)日期", "格式錯誤:" + account_close_date));
								}
							}

							// 帳戶餘額正負號 R X(01)*
							String balance_acct_currency_sign = strQueue.popBytesString(1);
							data.setBalance_acct_currency_sign(balance_acct_currency_sign);

							if (ETL_Tool_FormatCheck.isEmpty(balance_acct_currency_sign)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳戶餘額正負號", "空值"));
							} else if (!checkMaps.get("balance_acct_currency_sign")
									.containsKey(balance_acct_currency_sign.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"帳戶餘額正負號", "非預期:" + balance_acct_currency_sign));
							}

							// 帳戶餘額 R 9(12)V99*
							String balance_acct_currency_value = strQueue.popBytesString(14);

							// 因為ETL_Tool_StringX.strToBigDecimal 有做例外處理，例外狀況會傳回0，所以不用再自行塞預設值
							data.setBalance_acct_currency_value(
									ETL_Tool_StringX.strToBigDecimal(balance_acct_currency_value, 2));

							if (ETL_Tool_FormatCheck.isEmpty(balance_acct_currency_value)) {
								// 預設值 - 0
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "帳戶餘額", "空值");
								errorLog_Data.setDEFAULT_VALUE("0");
								errorList.add(errorLog_Data);
							} else if (!ETL_Tool_FormatCheck.checkNum(balance_acct_currency_value)) {
								// 預設值 - 0
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "帳戶餘額", "格式錯誤:" + balance_acct_currency_value);
								errorLog_Data.setDEFAULT_VALUE("0");
								errorList.add(errorLog_Data);
							}

							// 過去一個月平均餘額正負號 R X(01)*
							String balance_last_month_avg_sign = strQueue.popBytesString(1);
							data.setBalance_last_month_avg_sign(balance_last_month_avg_sign);

							if (ETL_Tool_FormatCheck.isEmpty(balance_last_month_avg_sign)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"過去一個月平均餘額正負號", "空值"));
							} else if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(balance_last_month_avg_sign)
									&& !checkMaps.get("balance_last_month_avg_sign")
											.containsKey(balance_last_month_avg_sign.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"過去一個月平均餘額正負號", "非預期:" + balance_last_month_avg_sign));
							}

							// 過去一個月平均餘額 R 9(12)V99*
							String balance_last_month_avg_value = strQueue.popBytesString(14);

							// 因為ETL_Tool_StringX.strToBigDecimal 有做例外處理，例外狀況會傳回0，所以不用再自行塞預設值
							data.setBalance_last_month_avg_value(
									ETL_Tool_StringX.strToBigDecimal(balance_last_month_avg_value, 2));

							if (ETL_Tool_FormatCheck.isEmpty(balance_last_month_avg_value)) {
								// 預設值 - 0
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "過去一個月平均餘額", "空值");
								errorLog_Data.setDEFAULT_VALUE("0");
								errorList.add(errorLog_Data);
							} else if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(balance_last_month_avg_value)) {
								if (!ETL_Tool_FormatCheck.checkNum(balance_last_month_avg_value)) {
									// 預設值 - 0
									transDataErrorMark(data, "W");
									errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "過去一個月平均餘額",
											"格式錯誤:" + balance_last_month_avg_value);
									errorLog_Data.setDEFAULT_VALUE("0");
									errorList.add(errorLog_Data);
								}
							}

							// 警示註記 O X(02)
							String caution_note = strQueue.popBytesString(2);
							data.setCaution_note(caution_note);

							if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(caution_note)
									&& !checkMaps.get("caution_note").containsKey(caution_note.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"警示註記", "非預期:" + caution_note));
							}

							// ErrorList統一寫入PK值, 再寫入Error Log
							String tempSRC_Data = "";
							for (int j = 0; j < errorList.size(); j++) {
								// 寫入pk值
								errorList.get(j).setDOMAIN_ID(domain_id);
								errorList.get(j).setPARTY_NUMBER(party_number);
								errorList.get(j).setACCOUNT_ID(account_id);
								
								// 第一筆時，將轉換String預存
								if (j == 0) {
									// 寫入整行String - 無難字轉換
									tempSRC_Data = strQueue.getAllString();
								}
								// 寫入整行String
								errorList.get(j).setSRC_DATA(tempSRC_Data);
								
								// 寫入一個Error
								errWriter.addErrLog(errorList.get(j));
								
								// 最後一筆時將預存List清空
								if (j == (errorList.size() - 1)) {
									// 將List清空
									errorList.clear();
								}
							}

							// data list 加入一個檔案
							addData(data);

							if ("Y".equals(data.getError_mark())) {
								failureCount++;
							} else if ("W".equals(data.getError_mark())) {
								// 若參數為"W" 計入warning Count
								warningCount++;
								// 將Error_Mark改為"", 視為正常繼續ETL作業
								transDataErrorMark(data, "");
							} else {
								successCount++;
							}

							// 實際處理明細錄筆數累加
							grandTotal += 1;

							// System.out.println("實際處理列數:" + rowCount + " / 實際處理明細錄筆數:" + grandTotal + " / 目前處理資料第" + strQueue.getBytesListIndex() + "筆");

							rowCount++; // 處理行數 + 1

							/*
							 * 第一個條件是 初次處理，且資料總筆數比制定範圍大時 會進入條件
							 * 第二個條件是非初次處理，且個別資料來源已處理的筆數，可以被制定範圍整除時進入
							 */
							if ((isFirstTime && (isFileOK >= ETL_Profile.ETL_E_Stage)
									&& grandTotal == (ETL_Profile.ETL_E_Stage - 1))
									|| (!isFirstTime && (strQueue.getBytesListIndex() % ETL_Profile.ETL_E_Stage == 0))

							) {

								// System.out.println("=======================================");
								//
								// if (isFirstTime)
								// System.out.println("第一次處理，資料來源須扣除首錄筆數");
								// 記錄非初次
								isFirstTime = false;

								// System.out
								// .println("累積處理資料已達到限制處理筆數範圍:" +
								// ETL_Profile.ETL_E_Stage +
								// "筆，再度切割資料來源進入QUEUE");

								// 注入指定範圍筆數資料到QUEUE
								strQueue.setBytesList(fileByteUtil.getFilesBytes());
								// 初始化使用筆數
								strQueue.setBytesListIndex(0);

								// System.out.println("初始化提取處理資料，目前處理資料為:" +
								// strQueue.getBytesListIndex());
								// System.out.println("=======================================");
							}
						}
					}

					// Account_Data寫入DB
					insert_Account_Datas();

					// 修正筆數, 考慮寫入資料庫時寫入失敗的狀況
					successCount = successCount - this.oneFileInsertErrorCount;
					failureCount = failureCount + this.oneFileInsertErrorCount;
					// 單一檔案寫入DB error個數重計
					this.oneFileInsertErrorCount = 0;

					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						strQueue.setTargetString();
						
						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 90 = 113)
						if (strQueue.getTotalByteLength() != 113) {
							fileFmtErrMsg = "尾錄位元數非預期113:" + strQueue.getTotalByteLength();
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別碼檢核(1)
						String typeCode = strQueue.popBytesString(1);
						if (!"3".equals(typeCode)) {
							fileFmtErrMsg = "尾錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}
						
						// 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) {
							fileFmtErrMsg = "尾錄報送單位代碼與檔名不符:" + central_no;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}

						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (record_date == null || "".equals(record_date.trim())) {
							fileFmtErrMsg = "尾錄檔案日期空值";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!record_date.equals(pfn.getRecord_Date_String())) {
							fileFmtErrMsg = "尾錄檔案日期與檔名不符:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "尾錄檔案日期格式錯誤:" + record_date;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 總筆數檢核(7)
						String totalCount = strQueue.popBytesString(7);

						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤:" + totalCount;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符:" + totalCount + "!=" + (rowCount - 2);
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}

						// 保留欄檢核(90)
						strQueue.popBytesString(90);

						// 程式統計檢核
						if ((rowCount - 2) != (successCount + warningCount + failureCount)) {
							fileFmtErrMsg = "總筆數 <> 成功比數   + 警示筆數 + 失敗筆數";
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "程式檢核", fileFmtErrMsg));
						}

					}

					Date parseEndDate = new Date(); // 開始執行時間
					System.out.println("解析檔案： " + fileName + " End " + parseEndDate);

					// 執行結果
					String file_exe_result;
					// 執行結果說明
					String file_exe_result_description;

					if (!"".equals(fileFmtErrMsg)) {
						file_exe_result = "S";
						file_exe_result_description = "解析檔案出現嚴重錯誤";
						processErrMsg = processErrMsg + pfn.getFileName() + "解析檔案出現嚴重錯誤\n";
					} else if (failureCount == 0) {
						file_exe_result = "Y";
						file_exe_result_description = "執行結果無錯誤資料";
					} else {
						file_exe_result = "D";
						file_exe_result_description = "錯誤資料筆數: " + failureCount;
					}

					// Error_Log寫入DB
					errWriter.insert_Error_Log();

					// 處理後更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseEndDate,
							(successCount + warningCount + failureCount), successCount, warningCount, failureCount, file_exe_result, file_exe_result_description);
				} catch (Exception ex) {
					// 發生錯誤時, 資料List & 計數 reset
					this.dataCount = 0;
					this.dataList.clear();

					// 寫入Error_Log
					ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
							"E", "0", "ETL_E_ACCOUNT程式處理", ex.getMessage(), null);

					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", new Date(), 0, 0, 0, 0, "S",
							ex.getMessage());
					processErrMsg = processErrMsg + ex.getMessage() + "\n";

					ex.printStackTrace();
				}
				// 累加處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;
			}
			
			// 過濾軌跡資料
			try {

				ETL_P_EData_Filter.E_Datas_Filter("filter_Account_Temp_Temp", batch_no, exc_central_no, exc_record_date, upload_no, program_no);

			} catch (Exception ex) {
				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
						"0", "ETL_E_ACCOUNT程式處理", ex.getMessage(), null);

				processErrMsg = processErrMsg + ex.getMessage() + "\n";

				ex.printStackTrace();
			}

			// 執行結果
			String detail_exe_result;
			// 執行結果說明
			String detail_exe_result_description;

			if (fileList.size() == 0) {
				detail_exe_result = "S";
				detail_exe_result_description = "缺檔案類型：" + fileTypeName + " 檔案";

				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
						"0", "ETL_E_ACCOUNT程式處理", detail_exe_result_description, null);

			} else if (!"".equals(processErrMsg)) {
				detail_exe_result = "S";
				detail_exe_result_description = processErrMsg;
			} else if (detail_ErrorCount == 0) {
				detail_exe_result = "Y";
				detail_exe_result_description = "檔案轉入檢核無錯誤";
			} else {
				detail_exe_result = "N";
				detail_exe_result_description = "錯誤資料筆數: " + detail_ErrorCount;
			}

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", detail_exe_result, detail_exe_result_description, new Date());

		} catch (Exception ex) {
			// 寫入Error_Log
			ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
					"0", "ETL_E_ACCOUNT程式處理", ex.getMessage(), null); 

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_ACCOUNT - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_ACCOUNT_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Account_Datas();
		}
	}

	// 將ACCOUNT資料寫入資料庫
	private void insert_Account_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_ACCOUNT - insert_Account_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫ACCOUNT寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_ACCOUNT_TEMP(?,?)}");
		// DB2 type - ACCOUNT
		insertAdapter.setCreateStructTypeName("T_ACCOUNT");
		// DB2 array type - ACCOUNT
		insertAdapter.setCreateArrayTypesName("A_ACCOUNT");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.dataList, insertAdapter); 
		int errorCount = insertAdapter.getErrorCount(); 

		if (isSuccess) {
			System.out.println("insert_Account_Datas 寫入 " + this.dataList.size() + "(-" + errorCount + ")筆資料!"); 
			this.oneFileInsertErrorCount = this.oneFileInsertErrorCount + errorCount;
		} else {
			throw new Exception("insert_Account_Datas 發生錯誤");
		}

		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}

	// 轉換Error_Mark
	private static void transDataErrorMark(ETL_Bean_ACCOUNT_Data data, String trans_Error_Mark) {
		if (data != null) {
			// 欲將Error_Mark改成W
			if ("W".equals(trans_Error_Mark)) {
				// 已經是"Y"的不進行變更, 反之更新為"W"
				if ("Y".equals(data.getError_mark())) {
					return;
				} else {
					data.setError_mark(trans_Error_Mark);
					return;
				}
			} else {
				// 其餘直接轉換
				data.setError_mark(trans_Error_Mark);
				return;
			}
		}
	}

	public static void main(String[] argv) throws Exception {

		// 讀取測試資料，並只列出明細錄欄位
		// Charset charset = Charset.forName("Big5");
		// List<String> lines = Files
		// .readAllLines(Paths.get("D:\\PSC\\Projects\\AgriBank\\單元測試報告\\018_FR_ACCOUNT_20180116.txt"),
		// charset);
		//
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
		// System.out.println("異動代號X(01): " + new String(Arrays.copyOfRange(tmp,
		// 19, 20), "Big5"));
		// System.out.println("帳號X(30): " + new String(Arrays.copyOfRange(tmp,
		// 20, 50), "Big5"));
		// System.out.println("帳戶行X(07): " + new String(Arrays.copyOfRange(tmp,
		// 50, 57), "Big5"));
		// System.out.println("帳戶類別X(02): " + new String(Arrays.copyOfRange(tmp,
		// 57, 59), "Big5"));
		// System.out.println("連結服務X(01): " + new String(Arrays.copyOfRange(tmp,
		// 59, 60), "Big5"));
		// System.out.println("幣別X(03): " + new String(Arrays.copyOfRange(tmp,
		// 60, 63), "Big5"));
		// System.out.println("帳戶狀態X(01): " + new String(Arrays.copyOfRange(tmp,
		// 63, 64), "Big5"));
		// System.out.println("開戶管道X(01): " + new String(Arrays.copyOfRange(tmp,
		// 64, 65), "Big5"));
		// System.out.println("開戶日期X(08): " + new String(Arrays.copyOfRange(tmp,
		// 65, 73), "Big5"));
		// System.out.println("結清(銷戶)日期X(08): " + new
		// String(Arrays.copyOfRange(tmp, 73, 81), "Big5"));
		// System.out.println("帳戶餘額正負號X(01): " + new
		// String(Arrays.copyOfRange(tmp, 81, 82), "Big5"));
		// System.out.println("帳戶餘額9(12)V99: " + new
		// String(Arrays.copyOfRange(tmp, 82, 96), "Big5"));
		// System.out.println("過去一個月平均餘額正負號X(01): " + new
		// String(Arrays.copyOfRange(tmp, 96, 97), "Big5"));
		// System.out.println("過去一個月平均餘額9(12)V99: " + new
		// String(Arrays.copyOfRange(tmp, 97, 111), "Big5"));
		// System.out.println("警示註記X(02): " + new String(Arrays.copyOfRange(tmp,
		// 111, 113), "Big5"));
		// System.out.println(
		// "============================================================================================");
		// }
		// }

		// 讀取測試資料，並運行程式
		ETL_E_ACCOUNT one = new ETL_E_ACCOUNT();
		String filePath = "D:\\PSC\\Projects\\AgriBank\\UNIT_TEST";
		// String filePath =
		// "D:\\PSC\\Projects\\AgriBank\\UNIT_TEST\\data\\五代查詢\\0409_600";
		String fileTypeName = "ACCOUNT";
		one.read_Account_File(filePath, fileTypeName, "ETL99908", "018",
				new SimpleDateFormat("yyyyMMdd").parse("20180412"), "001", "ETL_E_ACCOUNT");
	}

}
