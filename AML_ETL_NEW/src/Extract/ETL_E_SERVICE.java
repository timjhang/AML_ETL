package Extract;

import java.io.File;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_SERVICE_TEMP_Data;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.ETL_Q_ColumnCheckCodes;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FileByteUtil;
import Tool.ETL_Tool_FileFormat;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ParseFileName;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_E_SERVICE extends Extract {
	
	public ETL_E_SERVICE() {
		
	}

	public ETL_E_SERVICE(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) {
		super(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
	}

	@Override
	public void read_File() {
		try {
			read_Service_File(this.filePath, this.fileTypeName, this.batch_no, this.exc_central_no,
					this.exc_record_date, this.upload_no, this.program_no);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "T_4", "COMM_DOMAIN_ID" }, // 金融機構代號
			{ "T_9", "SERVICE_SERVICE_TYPE" } // 服務資料_服務類別
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
	private List<ETL_Bean_SERVICE_TEMP_Data> dataList = new ArrayList<ETL_Bean_SERVICE_TEMP_Data>();

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_Service_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {
		
		// TODO V6_2 start
		// 取得所有檢核用子map, 置入母map內
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(exc_record_date, exc_central_no, checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_SERVICE 抓取checkMaps資料有誤!"); // TODO V6_2
			ex.printStackTrace();
		}
		// TODO V6_2 end

		System.out.println("#######Extrace - ETL_E_SERVICE - Start");

		try {
			// 批次不重複執
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_SERVICE - 不重複執行\n" + inforMation); // TODO V4
				System.out.println("#######Extrace - ETL_E_SERVICE - End"); // TODO V4

				return;
			}

			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理錯誤計數
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
						ETL_E_SERVICE.class);

				// 檔名
				String fileName = parseFile.getName();

				// 讀檔檔名英文字轉大寫比較
				if (!ETL_Tool_FormatCheck.isEmpty(fileName))
					fileName = fileName.toUpperCase();
				
				Date parseStartDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " Start " + parseStartDate);

				// 解析fileName物件
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName);
				// 設定批次編號 // TODO V4 搬家
				pfn.setBatch_no(batch_no);
				// 設定上傳批號 // TODO V4
				pfn.setUpload_no(upload_no);

				// 報送單位非預期, 不進行解析
				if (exc_central_no == null || "".equals(exc_central_no.trim())) {
					System.out.println("## ETL_E_SERVICE - read_SERVICE_File - 控制程式無提供報送單位，不進行解析！"); // TODO V3
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
					System.out.println("## ETL_E_SERVICE - read_SERVICE_File - 控制程式無提供資料日期，不進行解析！"); // TODO V3
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
				// 警示計數(預設值) // TODO V11
				int warningCount = 0;
				// 失敗計數
				int failureCount = 0;
				// error_log data // TODO V11
				ETL_Bean_ErrorLog_Data errorLog_Data;

				// 紀錄是否第一次
				boolean isFirstTime = false;

				try {

					// 開始前ETL_FILE_Log寫入DB
					ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseStartDate, null, 0, 0, 0, 0,
							pfn.getFileName());

					// 嚴重錯誤訊息變數
					String fileFmtErrMsg = "";

					// ETL_字串處理Queue // TODO V4
					ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue(exc_central_no);
					// ETL_Error Log寫入輔助工具
					ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();

					int isFileOK = fileByteUtil.isFileOK(pfn, upload_no, parseFile.getAbsolutePath());

					boolean isFileFormatOK = isFileOK != 0 ? true : false;

					fileFmtErrMsg = isFileFormatOK ? "" : "區別碼錯誤";

					// 首錄檢查
					if (isFileFormatOK) {

						// TODO V5 START
						// 注入指定範圍筆數資料到QUEUE
						strQueue.setBytesList(fileByteUtil.getFilesBytes());
						// TODO V5 END

						// strQueue工具注入第一筆資料 // TODO V4
						strQueue.setTargetString();

						// 檢查整行bytes數(1 + 7 + 8 + 1418 = 1434)
						if (strQueue.getTotalByteLength() != 1434) {
							fileFmtErrMsg = "首錄位元數非預期1434:" + strQueue.getTotalByteLength();// TODO V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別瑪檢核(1)
						String typeCode = strQueue.popBytesString(1);
						if (!"1".equals(typeCode)) { // 首錄區別碼檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄報送單位代碼與檔名不符:" + central_no;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "報送單位", fileFmtErrMsg));
						}

						// 檔案日期檢核(8)
						String record_date = strQueue.popBytesString(8);
						if (record_date == null || "".equals(record_date.trim())) {
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

						// 保留欄位檢核(1418)
						String keepColumn = strQueue.popBytesString(1418);

						rowCount++; // 處理行數 + 1
					}

					// 實際處理明細錄筆數
					int grandTotal = 0;

					// 明細錄檢查- 逐行讀取檔案
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行 TODO
						if (rowCount == 2)
							isFirstTime = true;
						// 以實際處理明細錄筆數為依據，只運行明細錄次數
						while (grandTotal < (isFileOK - 2)) {

							strQueue.setTargetString();

							// 生成一個Data
							ETL_Bean_SERVICE_TEMP_Data data = new ETL_Bean_SERVICE_TEMP_Data(pfn);
							data.setRow_count(rowCount);

							// 整行bytes數檢核(1 + 7 + 11 + 16 + 8 + 6+10+10+20+75+400+400+400+20+50 = 1434)
							if (strQueue.getTotalByteLength() != 1434) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "行數bytes檢查", "非預期1434"));

								failureCount++;
								rowCount++; // 處理行數 ++
								grandTotal++; //TODO V6
								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								continue;
							}

							// TODO V11  寫入ErrorList
							List<ETL_Bean_ErrorLog_Data> errorList = new ArrayList<ETL_Bean_ErrorLog_Data>();

							// 區別碼檢核 *
							String typeCode = strQueue.popBytesString(1); // TODO V4
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "區別碼", "非預期:" + typeCode));
							}

							// 本會代號檢核(7) T_4*
							String domain_id = strQueue.popBytesString(7);
							data.setDomain_id(domain_id);
							if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "空值"));
							} else if (!checkMaps.get("T_4").containsKey(domain_id.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "非預期:" + domain_id));
							}

							// 客戶統編 X(11) *
							String party_number = strQueue.popBytesString(11);
							data.setParty_number(party_number);
							if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶統編", "空值"));
							}

							// 服務編號 X(16) *
							String service_id = strQueue.popBytesString(16);
							data.setService_id(service_id);
							if (ETL_Tool_FormatCheck.isEmpty(service_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "服務編號", "空值"));
							}

							// 預設值 - 更新
							// 服務日期 X(08) * 格式yyyyMMdd。需等於首錄中檔案日期。
							String service_date = strQueue.popBytesString(8);
							if (ETL_Tool_FormatCheck.isEmpty(service_date)) {
//								data.setError_mark("Y");
								data.setService_date(ETL_Tool_StringX.toUtilDate(pfn.getRecord_Date_String()));
								transDataErrorMark(data, "W");
								errorLog_Data = new  ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "服務日期", "空值");
								errorLog_Data.setDEFAULT_VALUE(pfn.getRecord_Date_String());
								errorList.add(errorLog_Data);
							} else if (ETL_Tool_FormatCheck.checkDate(service_date)) {
								data.setService_date(ETL_Tool_StringX.toUtilDate(service_date));
//								if (!service_date.equals(pfn.getRecord_Date_String())) {
//									data.setError_mark("Y");
//									errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
//											String.valueOf(rowCount), "服務日期", "不等於首錄中檔案日期:" + service_date));
//								}
							} else {
								data.setService_date(ETL_Tool_StringX.toUtilDate(pfn.getRecord_Date_String()));
								transDataErrorMark(data, "W");
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "服務日期", "日期格式錯誤:" + service_date);
								errorLog_Data.setDEFAULT_VALUE(pfn.getRecord_Date_String());
								errorList.add(errorLog_Data);
							}

							// 預設值 - 更新 1900-01-01 00:00:00
							// 服務時間 X(06) *
							String service_time = strQueue.popBytesString(6);
							if (ETL_Tool_FormatCheck.isEmpty(service_time)) {
								//data.setError_mark("Y");
								data.setService_time(
										new Time(ETL_Tool_StringX.toTimestamp("000000", "HHmmss").getTime()));
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "服務時間", "空值");

								errorLog_Data.setDEFAULT_VALUE("000000");
								errorList.add(errorLog_Data);

							} else if (ETL_Tool_FormatCheck.checkDate(service_time, "HHmmss")) {
								data.setService_time(
										new Time(ETL_Tool_StringX.toTimestamp(service_time, "HHmmss").getTime()));
							} else {
								//data.setError_mark("Y");

								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "服務時間", "時間格式錯誤:" + service_time);

								data.setService_time(
										new Time(ETL_Tool_StringX.toTimestamp("000000", "HHmmss").getTime()));

								errorLog_Data.setDEFAULT_VALUE("000000");
								errorList.add(errorLog_Data);
							}

							// 服務類別 X(10) *T_9
							String service_type = strQueue.popBytesString(10);
							data.setService_type(service_type);
							if (ETL_Tool_FormatCheck.isEmpty(service_type)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "服務類別", "空值"));
							} else if (!checkMaps.get("T_9").containsKey(service_type.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "服務類別", "非預期:" + service_type));
							}

							// 服務管道類別 X(10)
							String channel_type = strQueue.popBytesString(10);
							data.setChannel_type(channel_type);

							// 服務管道編號 X(20)
							String channel_id = strQueue.popBytesString(20);
							data.setChannel_id(channel_id);

							// 服務參考編號 X(75)
							String service_reference_number = strQueue.popBytesString(75);
							data.setService_reference_number(service_reference_number);

							// 資料變更前詳細內容 X(400)
							String previous_value = strQueue.popBytesString(400);
							data.setPrevious_value(previous_value);

							// 資料變更後詳細內容 X(400)
							String new_value = strQueue.popBytesString(400);
							data.setNew_value(new_value);

							// 服務詳細內容 X(400)
							String service_description = strQueue.popBytesString(400);
							data.setService_description(service_description);

							// 服務執行分行 X(20)
							String execution_branch_code = strQueue.popBytesString(20);
							data.setExecution_branch_code(execution_branch_code);

							// 執行行員代號 X(50)
							String executer_id = strQueue.popBytesString(50);
							data.setExecuter_id(executer_id);
							
							// TODO V11  
							// ErrorList統一寫入PK值, 再寫入Error Log
							String tempSRC_Data = "";
							for (int j = 0; j < errorList.size(); j++) {
								
								// 寫入pk值
								errorList.get(j).setDOMAIN_ID(domain_id);
								errorList.get(j).setPARTY_NUMBER(party_number);
								errorList.get(j).setTRANSACTION_ID(service_id);

								if (j == 0) {
									tempSRC_Data = strQueue.getAllString(); // 無難字轉換
								}
								
								// 寫入整行String
								errorList.get(j).setSRC_DATA(tempSRC_Data);
								// 寫入一個Error
								errWriter.addErrLog(errorList.get(j));

								if (j == (errorList.size() - 1)) {
									// 將List清空
									errorList.clear();
								}
							}

							// data list 加入一個檔案
							addData(data);

							if ("Y".equals(data.getError_mark())) {
								failureCount++;
							}else if ("W".equals(data.getError_mark())) { // TODO V11  新增一個else if, 在 if 與else之間
								// 若參數為"W" 計入warning Count
								warningCount++;
								// 將Error_Mark改為"", 視為正常繼續ETL作業
								transDataErrorMark(data, "");
							} else {
								successCount++;
							}

							// TODO V5 START
							// 實際處理明細錄筆數累加
							grandTotal += 1;

							rowCount++; // 處理行數 + 1

							/*
							 * 第一個條件是 初次處理，且資料總筆數比制定範圍大時 會進入條件 第二個條件是非初次處理，且個別資料來源已處理的筆數，可以被制定範圍整除時進入
							 */
							if ((isFirstTime && (isFileOK >= ETL_Profile.ETL_E_Stage)
									&& grandTotal == (ETL_Profile.ETL_E_Stage - 1))
									|| (!isFirstTime && (strQueue.getBytesListIndex() % ETL_Profile.ETL_E_Stage == 0))

							) {

								// 記錄非初次
								isFirstTime = false;

								// 注入指定範圍筆數資料到QUEUE
								strQueue.setBytesList(fileByteUtil.getFilesBytes());
								// 初始化使用筆數
								strQueue.setBytesListIndex(0);

							}
							// TODO V5 END
						}
					}

					insert_SERVICE_TEMP_Datas();
					
					// TODO V6_2 start
					// 修正筆數, 考慮寫入資料庫時寫入失敗的狀況
					successCount = successCount - this.oneFileInsertErrorCount;
					failureCount = failureCount + this.oneFileInsertErrorCount;
					// 單一檔案寫入DB error個數重計
					this.oneFileInsertErrorCount = 0;
					// TODO V6_2 end

					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						// TODO V5 START
						strQueue.setTargetString();
						// TODO V5 END

						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 1411 = 1434)
						if (strQueue.getTotalByteLength() != 1434) {
							fileFmtErrMsg = "尾錄位元數非預期1434:" + strQueue.getTotalByteLength();
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別碼檢核(1) 經"逐行讀取檔案"區塊, 若無嚴重錯誤應為3, 此處無檢核
						String typeCode = strQueue.popBytesString(1);
						if (!"3".equals(typeCode)) {
							fileFmtErrMsg = "尾錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查, 嚴重錯誤, 不進行迴圈並記錄錯誤訊息
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
						// iTotalCount = ETL_Tool_StringX.toInt(totalCount);
						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤:" + totalCount;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符:" + totalCount + "!=" + (rowCount - 2);
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}

						// 保留欄檢核(1411)
						String keepColumn = strQueue.popBytesString(1411);

						// 程式統計檢核
						if ((rowCount - 2) != (successCount + warningCount +failureCount)) {
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

					//TODO V6 END
					if (!"".equals(fileFmtErrMsg)) {
						file_exe_result = "S";
						file_exe_result_description = "解析檔案出現嚴重錯誤";
						processErrMsg = processErrMsg + pfn.getFileName() + "解析檔案出現嚴重錯誤\n";
					} else if (failureCount == 0) {
						file_exe_result = "Y";
						file_exe_result_description = "執行結果無錯誤資料";
					} else {
						file_exe_result = "D";
						// file_exe_result_description = "錯誤資料筆數: " + detail_ErrorCount; // TODO V4
						file_exe_result_description = "錯誤資料筆數: " + failureCount; // TODO V4
					}

					// Error_Log寫入DB // TODO V4 搬家
					errWriter.insert_Error_Log();

					// 處理後更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseEndDate,
							(successCount + warningCount + failureCount), successCount, warningCount,failureCount, file_exe_result,
							file_exe_result_description);

				} catch (Exception ex) {
					// 發生錯誤時, 資料List & 計數 reset // TODO V6_2
					this.dataCount = 0; 
					this.dataList.clear();
					
					// 寫入Error_Log
					ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
							"E", "0", "ETL_E_SERVICE程式處理", ex.getMessage(), null); // TODO V4 NEW

					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", new Date(), 0, 0, 0, 0, "S",
							ex.getMessage()); // TODO V4 (0, 0, 0)<=(iTotalCount, successCount, failureCount)
					processErrMsg = processErrMsg + ex.getMessage() + "\n";

					ex.printStackTrace();
				}

				// 累加處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;

			}

			// 執行結果
			String detail_exe_result;
			// 執行結果說明
			String detail_exe_result_description;

			
			if (fileList.size() == 0) { // TODO V4
				detail_exe_result = "S";
				detail_exe_result_description = "缺檔案類型：" + fileTypeName + " 檔案";

				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
						"0", "ETL_E_SERVICE程式處理", detail_exe_result_description, null); // TODO V4 NEW

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
			// TODO V3 end

		} catch (Exception ex) {
			// TODO V4 NEW
			// 寫入Error_Log
			ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
					"0", "ETL_E_SERVICE程式處理", ex.getMessage(), null); // TODO V4 NEW

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_SERVICE - End"); //

	}

	// List增加一個data

	private void addData(ETL_Bean_SERVICE_TEMP_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_SERVICE_TEMP_Datas();
		}
	}

	private void insert_SERVICE_TEMP_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_SERVICE - insert_SERVICE_TEMP_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_SERVICE_TEMP(?,?)}"); // 呼叫寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_SERVICE_TEMP"); // DB2 array type
		insertAdapter.setCreateStructTypeName("T_SERVICE_TEMP"); // DB2 type
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.dataList, insertAdapter); // TODO V6_2
		int errorCount = insertAdapter.getErrorCount(); // TODO V6_2

		if (isSuccess) {
			System.out.println("insert_SERVICE_Datas 寫入 " + this.dataList.size() + "(-" + errorCount + ")筆資料!"); // TODO V6_2
			this.oneFileInsertErrorCount = this.oneFileInsertErrorCount + errorCount; // TODO V6_2
		} else {
			throw new Exception("insert_SERVICE_Datas 發生錯誤");
		}

		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}
	
	// TODO V11
	// 轉換Error_Mark
	private static void transDataErrorMark(ETL_Bean_SERVICE_TEMP_Data data, String trans_Error_Mark) {
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
		ETL_E_SERVICE program = new ETL_E_SERVICE();
		String filePath = "D:\\company\\pershing\\agribank\\test_data\\20181025";
		String fileTypeName = "SERVICE";
		String batch_no = "test18";
		String exc_central_no = "018";
		String exc_record_date = "20171220";
		String upload_no = "001";
		String program_no = "aaa";
		program.read_Service_File(filePath, fileTypeName, batch_no, exc_central_no,
				new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no, program_no);

	}

}
