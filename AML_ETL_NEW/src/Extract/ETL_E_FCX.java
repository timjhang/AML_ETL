package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_FCX_TEMP_Data;
import DB.ETL_P_Data_Writer;
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

public class ETL_E_FCX extends Extract {

	public ETL_E_FCX() {

	}

	public ETL_E_FCX(String filePath, String fileTypeName, String batch_no, String exc_central_no, Date exc_record_date,
			String upload_no, String program_no) {
		super(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
	}

	@Override
	public void read_File() {
		try {
			read_FCX_File(this.filePath, this.fileTypeName, this.batch_no, this.exc_central_no, this.exc_record_date,
					this.upload_no, this.program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "comm_file_type", "COMM_FILE_TYPE" }, // 業務別
			{ "T_4", "COMM_DOMAIN_ID" }, // 金融機構代號
			{ "T_8", "COMM_NATIONALITY_CODE" }, // 國籍
			{ "T_12", "FCX_DIRECTION" }, // 外幣現鈔買賣資料_結構或結售
			{ "T_13", "COMM_CURRENCY_CODE" }, // 幣別
			{ "T_16", "FCX_CHANNEL_TYPE" } }; // 外幣現鈔買賣資料_交易管道類別

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// insert errorLog fail Count
	private int oneFileInsertErrorCount = 0;

	// Data儲存List
	private List<ETL_Bean_FCX_TEMP_Data> dataList = new ArrayList<ETL_Bean_FCX_TEMP_Data>();

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
	public void read_FCX_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		// 取得所有檢核用子map, 置入母map內
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(exc_record_date, exc_central_no, checkMapArray);

		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_FCX 抓取checkMaps資料有誤!");
			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_FCX - Start");

		try {
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_FCX - 不重複執行\n" + inforMation);
				System.out.println("#######Extrace - ETL_E_FCX - End");

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
			List<File> fileList = ETL_Tool_FileReader.getTargetFileList_noFT(filePath, fileTypeName);

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
						ETL_E_FCX.class);

				// 檔名
				String fileName = parseFile.getName();

				// 讀檔檔名英文字轉大寫比較
				if (!ETL_Tool_FormatCheck.isEmpty(fileName))
					fileName = fileName.toUpperCase();

				Date parseStartDate = new Date(); // 開始執行時間
				System.out.println("解析檔案： " + fileName + " Start " + parseStartDate);

				// 解析fileName物件
				ETL_Tool_ParseFileName pfn = new ETL_Tool_ParseFileName(fileName, true);
				// 設定批次編號
				pfn.setBatch_no(batch_no);
				// 設定上傳批號
				pfn.setUpload_no(upload_no);

				// 報送單位非預期, 不進行解析
				if (exc_central_no == null || "".equals(exc_central_no.trim())) {
					System.out.println("## ETL_E_FCX - read_FCX_File - 控制程式無提供報送單位，不進行解析！"); 
					processErrMsg = processErrMsg + "控制程式無提供報送單位，不進行解析！\n";
					continue;
				} else if (!exc_central_no.trim().equals(pfn.getCentral_No().trim())) {
					System.out.println("##" + pfn.getFileName() + " 處理報送單位非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理報送單位非預期，不進行解析！\n";
					continue;
				}

				// 資料日期非預期, 不進行解析
				if (exc_record_date == null) {
					System.out.println("## ETL_E_FCX - read_FCX_File - 控制程式無提供資料日期，不進行解析！");
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

					// 嚴重錯誤訊息變數
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

						// 檢查整行bytes數(1 + 7 + 8 + 235 = 251)
						if (strQueue.getTotalByteLength() != 251) {
							fileFmtErrMsg = "首錄位元數非預期251" + strQueue.getTotalByteLength();
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
						if (record_date == null || "".equals(record_date.trim())) {
							fileFmtErrMsg = "首錄檔案日期空值:" + record_date;
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

						// 保留欄位檢核(235)
						strQueue.popBytesString(235);

						rowCount++; // 處理行數 + 1

					}

					// 實際處理明細錄筆數
					int grandTotal = 0;

					// 逐行讀取檔案
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { 
						
						if (rowCount == 2) {
							isFirstTime = true;
						}
						
						// 以實際處理明細錄筆數為依據，只運行明細錄次數
						while (grandTotal < (isFileOK - 2)) {

							strQueue.setTargetString();

							// 生成一個Data
							ETL_Bean_FCX_TEMP_Data data = new ETL_Bean_FCX_TEMP_Data(pfn);
							data.setRow_count(rowCount);

							// 整行bytes數檢核(1+7+11+80+8+2+20+8+14+1+3+14+2+10+20+50= 251)
							if (strQueue.getTotalByteLength() != 251) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "行數bytes檢查", "非預期251"));

								failureCount++;
								rowCount++; // 處理行數 ++
								grandTotal++;
								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								continue;
							}

							// 寫入ErrorList
							List<ETL_Bean_ErrorLog_Data> errorList = new ArrayList<ETL_Bean_ErrorLog_Data>();

							// 區別碼檢核 c-1*
							String typeCode = strQueue.popBytesString(1);
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"區別碼", "非預期: " + typeCode));
							}

							// 本會代號 X(07) * COMM_DOMAIN_ID
							String domain_id = strQueue.popBytesString(7);
							data.setDomain_id(domain_id);
							if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"本會代號", "空值"));
							} else if (!checkMaps.get("T_4").containsKey(domain_id.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"本會代號", "非預期:" + domain_id));
							}

							// 顧客統編 X(11) *
							String party_number = strQueue.popBytesString(11);
							data.setParty_number(party_number);
							if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"顧客統編", "空值"));
							}

							// 顧客姓名 X(80)
							String party_last_name_1 = strQueue.popBytesDiffString(80);
							data.setParty_last_name_1(party_last_name_1);

							// 顧客生日 X(08)
							String date_of_birth = strQueue.popBytesString(8);

							if (!ETL_Tool_FormatCheck.isEmpty(date_of_birth)) {
								if (ETL_Tool_FormatCheck.checkDate(date_of_birth)) {
									data.setDate_of_birth(ETL_Tool_StringX.toUtilDate(date_of_birth));
								} else if (advancedCheck) {
									// 預設值 - 19000101
									data.setDate_of_birth(ETL_Tool_StringX.toUtilDate("00000000"));
									transDataErrorMark(data, "W");
									errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "顧客生日", "日期格式錯誤:" + date_of_birth);
									errorLog_Data.setDEFAULT_VALUE("19000101");
									errorList.add(errorLog_Data);
								}

							}

							// 顧客國籍 X(02) COMM_NATIONALITY_CODE
							String nationality_code = strQueue.popBytesString(2);
							data.setNationality_code(nationality_code);
							if (ETL_Tool_FormatCheck.isEmpty(nationality_code)) {
								if (advancedCheck && !checkMaps.get("T_8").containsKey(nationality_code.trim())) {
									// 預設值 - TW
									data.setNationality_code("TW");
									transDataErrorMark(data, "W");
									errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "顧客國籍", "非預期:" + nationality_code);
									errorLog_Data.setDEFAULT_VALUE("TW");
									errorList.add(errorLog_Data);
								}
							}

							// 交易編號 X(20) *
							String transaction_id = strQueue.popBytesString(20);
							data.setTransaction_id(transaction_id);
							if (ETL_Tool_FormatCheck.isEmpty(transaction_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"交易編號", "空值"));
							}

							// 交易日期 X(08) *
							String transaction_date = strQueue.popBytesString(8);
							if (ETL_Tool_FormatCheck.isEmpty(transaction_date)) {
								// 預設值 - 首錄-檔案日期
								data.setTransaction_date(ETL_Tool_StringX.toUtilDate(pfn.getRecord_Date_String()));
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易日期", "空值");
								errorLog_Data.setDEFAULT_VALUE(pfn.getRecord_Date_String());
								errorList.add(errorLog_Data);
							} else if (ETL_Tool_FormatCheck.checkDate(transaction_date)) {
								data.setTransaction_date((ETL_Tool_StringX.toUtilDate(transaction_date)));
							} else {
								// 預設值 - 首錄-檔案日期
								data.setTransaction_date(ETL_Tool_StringX.toUtilDate(pfn.getRecord_Date_String()));
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易日期", "日期格式錯誤:" + transaction_date);
								errorLog_Data.setDEFAULT_VALUE(pfn.getRecord_Date_String());
								errorList.add(errorLog_Data);
							}

							// 實際交易時間 X(14) *
							String transaction_time = strQueue.popBytesString(14);
							if (ETL_Tool_FormatCheck.isEmpty(transaction_time)) {
								// 預設值 - 19000101000000
								data.setTransaction_time(ETL_Tool_StringX.toTimestamp("00000000000000"));
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "實際交易時間", "空值");
								errorLog_Data.setDEFAULT_VALUE("19000101000000");
								errorList.add(errorLog_Data);
							} else if (!ETL_Tool_FormatCheck.checkTimestamp(transaction_time)) {
								// 預設值 - 19000101000000
								data.setTransaction_time(ETL_Tool_StringX.toTimestamp("00000000000000"));
								transDataErrorMark(data, "W");
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "實際交易時間", "格式錯誤:" + transaction_time);
								errorLog_Data.setDEFAULT_VALUE("19000101000000");
								errorList.add(errorLog_Data);
							} else {
								data.setTransaction_time(ETL_Tool_StringX.toTimestamp(transaction_time));
							}

							// 結構或結售 X(01) FCX_DIRECTION *
							String direction = strQueue.popBytesString(1);
							data.setDirection(direction);
							if (ETL_Tool_FormatCheck.isEmpty(direction)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"結構或結售", "空值"));
							} else if (!checkMaps.get("T_12").containsKey(direction.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"結構或結售", "非預期:" + direction));
							}

							// 交易幣別 X(03) COMM_CURRENCY_CODE *
							String currency_code = strQueue.popBytesString(3);
							data.setCurrency_code(currency_code);
							if (ETL_Tool_FormatCheck.isEmpty(currency_code)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"交易幣別", "空值"));
							} else if (!checkMaps.get("T_13").containsKey(currency_code.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"交易幣別", "非預期:" + currency_code));
							}

							// 交易金額 9(12) V99 *
							String amount = strQueue.popBytesString(14);
							if (ETL_Tool_FormatCheck.isEmpty(amount)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"交易金額", "空值"));
							} else if (!ETL_Tool_FormatCheck.checkNum(amount)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"交易金額", "非數字:" + amount));
							} else {
								data.setAmount(ETL_Tool_StringX.strToBigDecimal(amount, 2));
							}

							// 申報國別 X(02) COMM_NATIONALITY_CODE *
							String ordering_customer_country = strQueue.popBytesString(2);
							data.setOrdering_customer_country(ordering_customer_country);
							if (ETL_Tool_FormatCheck.isEmpty(ordering_customer_country)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"申報國別", "空值"));
							} else if (!checkMaps.get("T_8").containsKey(ordering_customer_country.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
										"申報國別", "非預期:" + ordering_customer_country));
							}

							// 交易管道類別 X(10) FCX_CHANNEL_TYPE
							String channel_type = strQueue.popBytesString(10);
							data.setChannel_type(channel_type);
							if (!ETL_Tool_FormatCheck.isEmpty(channel_type)) {
								if (advancedCheck && !checkMaps.get("T_16").containsKey(channel_type.trim())) {
									data.setError_mark("Y");
									errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "交易管道類別", "非預期:" + channel_type));
								}
							}

							// 交易執行分行 X(20)
							String execution_branch_code = strQueue.popBytesString(20);
							data.setExecution_branch_code(execution_branch_code);

							// 交易執行者代號 X(50)
							String executer_id = strQueue.popBytesString(50);
							data.setExecuter_id(executer_id);

							// ErrorList統一寫入PK值, 再寫入Error Log
							String tempSRC_Data = "";
							for (int j = 0; j < errorList.size(); j++) {
								// 寫入pk值
								errorList.get(j).setDOMAIN_ID(domain_id);
								errorList.get(j).setPARTY_NUMBER(party_number);
								errorList.get(j).setTRANSACTION_ID(transaction_id);
								
								// 第一筆時，將轉換String預存
								if (j == 0) {
									// 寫入整行String - 有難字轉換
									tempSRC_Data = strQueue.getAllDiffString();
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

					insert_FCX_TEMP_Datas();

					// 修正筆數, 考慮寫入資料庫時寫入失敗的狀況
					successCount = successCount - this.oneFileInsertErrorCount;
					failureCount = failureCount + this.oneFileInsertErrorCount;
					// 單一檔案寫入DB error個數重計
					this.oneFileInsertErrorCount = 0;

					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						strQueue.setTargetString();

						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 228 = 251)
						if (strQueue.getTotalByteLength() != 251) {
							fileFmtErrMsg = "尾錄位元數非預期251:" + strQueue.getTotalByteLength();
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

						// 保留欄檢核(228)
						strQueue.popBytesString(228);

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
					ETL_P_Log.update_End_ETL_FILE_Log_NO_FILE_TYPE(pfn.getBatch_no(), pfn.getCentral_No(),
							exc_record_date, pfn.getFile_Name(), upload_no, "E", parseEndDate,
							(successCount + warningCount + failureCount), successCount, warningCount, failureCount,
							file_exe_result, file_exe_result_description);
				} catch (Exception ex) {
					// 發生錯誤時, 資料List & 計數 reset
					this.dataCount = 0;
					this.dataList.clear();

					// 寫入Error_Log
					ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
							"E", "0", "ETL_E_FCX程式處理", ex.getMessage(), null);

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

			// 執行結果
			String detail_exe_result;
			// 執行結果說明
			String detail_exe_result_description;

			if (fileList.size() == 0) { 
				detail_exe_result = "S";
				detail_exe_result_description = "缺檔案類型：" + fileTypeName + " 檔案";

				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
						"0", "ETL_E_FCX程式處理", detail_exe_result_description, null);

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
					"0", "ETL_E_FCX程式處理", ex.getMessage(), null);

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}
		System.out.println("#######Extrace - ETL_E_FCX - End");

	}

	// List增加一個data
	private void addData(ETL_Bean_FCX_TEMP_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_FCX_TEMP_Datas();
		}
	}

	private void insert_FCX_TEMP_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_FCX - insert_FCX_TEMP_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_FCX_TEMP(?,?)}"); // 呼叫寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_FCX_TEMP"); // DB2 array type
		insertAdapter.setCreateStructTypeName("T_FCX_TEMP"); // DB2 type
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.dataList, insertAdapter);
		int errorCount = insertAdapter.getErrorCount();

		if (isSuccess) {
			System.out.println("insert_FCX_Datas 寫入 " + this.dataList.size() + "(-" + errorCount + ")筆資料!"); 
			this.oneFileInsertErrorCount = this.oneFileInsertErrorCount + errorCount;
		} else {
			throw new Exception("insert_FCX_Datas 發生錯誤");
		}

		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}

	// 轉換Error_Mark
	private static void transDataErrorMark(ETL_Bean_FCX_TEMP_Data data, String trans_Error_Mark) {
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
		ETL_E_FCX program = new ETL_E_FCX();

		String filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no;

		try {
			filePath = "D:\\company\\pershing\\agribank\\test_data\\test";
			fileTypeName = "FCX";
			batch_no = "9985";
			exc_central_no = "600";
			exc_record_date = "20171206";
			upload_no = "900";
			program_no = "o";
			program.read_FCX_File(filePath, fileTypeName, batch_no, exc_central_no,
					new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no, program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
