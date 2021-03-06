package Extract;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_TRANSFER_TEMP_Data;
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

public class ETL_E_TRANSFER extends Extract {

	public ETL_E_TRANSFER() {

	}

	public ETL_E_TRANSFER(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) {
		super(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
	}

	@Override
	public void read_File() {
		try {
			read_Transfer_File(this.filePath, this.fileTypeName, this.batch_no, this.exc_central_no,
					this.exc_record_date, this.upload_no, this.program_no);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = { { "comm_file_type", "COMM_FILE_TYPE" }, { "T_4", "COMM_DOMAIN_ID" }, // 金融機構代號
			{ "T_9", "TRANSFER_DIRECTION" }, // 顧客境外交易資料_匯入或匯出
			{ "T_10", "COMM_CURRENCY_CODE" }, // 幣別
			{ "BIC", "COMM_NATIONALITY_CODE" }// 國籍代碼檢驗
	};

	// 欄位檢核用母Map
	private Map<String, Map<String, String>> checkMaps;

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	// insert errorLog fail Count // TODO V6_2
	private int oneFileInsertErrorCount = 0;

	// Data儲存List
	private List<ETL_Bean_TRANSFER_TEMP_Data> dataList = new ArrayList<ETL_Bean_TRANSFER_TEMP_Data>();

	// 讀取檔案
	// 根據(1)代號 (2)年月日yyyyMMdd, 開啟讀檔路徑中符合檔案
	// 回傳boolean 成功(true)/失敗(false)
	public void read_Transfer_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		// TODO V6_2 start
		// 取得所有檢核用子map, 置入母map內
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(exc_record_date, exc_central_no, checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_TRANSFER 抓取checkMaps資料有誤!"); // TODO V6_2
			ex.printStackTrace();
		}
		// TODO V6_2 end

		System.out.println("#######Extrace - ETL_E_TRANSFER - Start");

		try {

			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_TRANSFER - 不重複執行\n" + inforMation); // TODO V4
				System.out.println("#######Extrace - ETL_E_TRANSFER - End"); // TODO V4

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
						ETL_E_TRANSFER.class);// TODO
				// 檔名
				String fileName = parseFile.getName();
				// TODO V5 START
				// 讀檔檔名英文字轉大寫比較
				if (!ETL_Tool_FormatCheck.isEmpty(fileName))
					fileName = fileName.toUpperCase();
				// TODO V5 END
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
					System.out.println("## ETL_E_TRANSFER - read_TRANSFER_File - 控制程式無提供報送單位，不進行解析！"); // TODO V3
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
					System.out.println("## ETL_E_TRANSFER - read_TRANSFER_File - 控制程式無提供資料日期，不進行解析！"); // TODO V3
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
				// TODO V5 END

				try { // TODO V3

					// 開始前ETL_FILE_Log寫入DB // TODO V3
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
					// TODO V6 END
					boolean isFileFormatOK = isFileOK != 0 ? true : false;
					// TODO V5 END

					fileFmtErrMsg = isFileFormatOK ? "" : "區別碼錯誤";

					// 首錄檢查
					if (isFileFormatOK) {

						// TODO V5 START
						// 注入指定範圍筆數資料到QUEUE
						strQueue.setBytesList(fileByteUtil.getFilesBytes());
						// TODO V5 END

						// strQueue工具注入第一筆資料 // TODO V4
						strQueue.setTargetString();

						// 檢查整行bytes數(1 + 7 + 8 + 707 = 723)
						if (strQueue.getTotalByteLength() != 723) {
							fileFmtErrMsg = "首錄位元數非預期723:" + strQueue.getTotalByteLength();
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

						// 保留欄位檢核(707)
						String keepColumn = strQueue.popBytesString(707);

						rowCount++; // 處理行數 + 1
					}

					// TODO V5 START
					// 實際處理明細錄筆數
					int grandTotal = 0;
					// TODO V5 END

					// TODO V5 START
					// 明細錄檢查- 逐行讀取檔案
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行 TODO
						if (rowCount == 2)
							isFirstTime = true;
						// System.out.println("資料總筆數:" + isFileOK);
						// while (strQueue.setTargetString() < strQueue.getByteListSize()) {
						// 以實際處理明細錄筆數為依據，只運行明細錄次數
						while (grandTotal < (isFileOK - 2)) {

							strQueue.setTargetString();

							// TODO V5 END

							// 生成一個Data
							ETL_Bean_TRANSFER_TEMP_Data data = new ETL_Bean_TRANSFER_TEMP_Data(pfn);
							data.setRow_count(rowCount);

							// 整行bytes數檢核(1 + 7 + 11 + 50 + 8 + 14 + 1 + 3 + 14 + 50 + 50 + 80 + 100 + 20 +
							// 50 + 80 + 50 + 20 + 4 + 10 + 10 + 20 + 20 + 50 = 723)
							if (strQueue.getTotalByteLength() != 723) {
								data.setError_mark("Y");
								errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "行數bytes檢查", "非預期723"));

								failureCount++;
								rowCount++; // 處理行數 ++
								grandTotal++; // TODO V6
								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								continue;
							}

							// TODO V11 寫入ErrorList
							List<ETL_Bean_ErrorLog_Data> errorList = new ArrayList<ETL_Bean_ErrorLog_Data>();

							// 區別碼 X(01) *
							String typeCode = strQueue.popBytesString(1);
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "區別碼", "非預期:" + typeCode));
							}
							// 本會代號 X(07) T_4 *
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

							// 匯款編號(交易編號) X(50) *
							String transfer_id = strQueue.popBytesString(50);
							data.setTransfer_id(transfer_id);
							if (ETL_Tool_FormatCheck.isEmpty(transfer_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯款編號", "空值"));
							}

							// 匯款日期 X(08) * 預設 首錄-檔案日期
							String transfer_date = strQueue.popBytesString(8);
							if (ETL_Tool_FormatCheck.isEmpty(transfer_date)) {
//								data.setError_mark("Y");
//								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
//										String.valueOf(rowCount), "匯率日期", "空值"));
								
								data.setTransfer_date(ETL_Tool_StringX.toUtilDate(pfn.getRecord_Date_String()));
								transDataErrorMark(data, "W");
								
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",String.valueOf(rowCount), "匯率日期", "空值");
								
								errorLog_Data.setDEFAULT_VALUE(pfn.getRecord_Date_String());
								errorList.add(errorLog_Data);

							} else if (ETL_Tool_FormatCheck.checkDate(transfer_date)) {
								data.setTransfer_date(ETL_Tool_StringX.toUtilDate(transfer_date));
							} else {
//								data.setError_mark("Y");
//								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
//										String.valueOf(rowCount), "匯率日期", "日期格式錯誤:" + transfer_date));
								
								data.setTransfer_date(ETL_Tool_StringX.toUtilDate(pfn.getRecord_Date_String()));
								transDataErrorMark(data, "W");
								
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯率日期", "日期格式錯誤:" + transfer_date);
								
								errorLog_Data.setDEFAULT_VALUE(pfn.getRecord_Date_String());
								errorList.add(errorLog_Data);
							}

							// 實際匯款時間 X(14) * 預設 1900-01-01 00:00:00
							String transfer_time = strQueue.popBytesString(14);
							if (ETL_Tool_FormatCheck.isEmpty(transfer_time)) {
								//data.setError_mark("Y");

								data.setTransfer_time(ETL_Tool_StringX.toTimestamp("19000101000000"));
								transDataErrorMark(data, "W");
								
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "實際匯款時間", "空值");
								errorLog_Data.setDEFAULT_VALUE("19000101000000");
								errorList.add(errorLog_Data);

							} else if (ETL_Tool_FormatCheck.checkDate(transfer_time, "yyyyMMddHHmmss")) {
								data.setTransfer_time(ETL_Tool_StringX.toTimestamp(transfer_time));
							} else {								
								data.setTransfer_time(ETL_Tool_StringX.toTimestamp("19000101000000"));
								transDataErrorMark(data, "W");
								
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "實際匯款時間", "時間格式錯誤:" + transfer_time);
								errorLog_Data.setDEFAULT_VALUE("19000101000000");
								errorList.add(errorLog_Data);
							}

							// 匯入或匯出 X(01) T_9 *
							String direction = strQueue.popBytesString(1);
							data.setDirection(direction);
							if (ETL_Tool_FormatCheck.isEmpty(direction)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯入或匯出", "空值"));
							} else if (!checkMaps.get("T_9").containsKey(direction.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯入或匯出", "非預期:" + direction));
							}

							// 交易幣別 X(03) T_10 *
							String instructed_currency_code = strQueue.popBytesString(3);
							data.setInstructed_currency_code(instructed_currency_code);
							if (ETL_Tool_FormatCheck.isEmpty(instructed_currency_code)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易幣別", "空值"));
							} else if (!checkMaps.get("T_10").containsKey(instructed_currency_code.trim())) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易幣別", "非預期:" + instructed_currency_code));
							}

							// 交易金額 9(12)V99 *
							String instructed_amount = strQueue.popBytesString(14);
							if (ETL_Tool_FormatCheck.isEmpty(instructed_amount)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易金額", "空值"));
							} else if (!ETL_Tool_FormatCheck.checkNum(instructed_amount)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易金額", "非數字:" + instructed_amount));
							} else {
								data.setInstructed_amount(ETL_Tool_StringX.strToBigDecimal(instructed_amount, 2));
							}

							// 匯款人銀行帳戶編號 X(50) * 預設 N/A
							String ordering_customer_account_number = strQueue.popBytesString(50);
							data.setOrdering_customer_account_number(ordering_customer_account_number);
							if (ETL_Tool_FormatCheck.isEmpty(ordering_customer_account_number)) {
								data.setOrdering_customer_account_number("N/A");
								transDataErrorMark(data, "W");
								
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯款人銀行帳戶編號", "空值");
								errorLog_Data.setDEFAULT_VALUE("N/A");
								errorList.add(errorLog_Data);
							}

							// 匯款人顧客編號 X(50)
							String ordering_customer_party_id = strQueue.popBytesString(50);
							data.setOrdering_customer_party_id(ordering_customer_party_id);

							// 匯款人顧客姓名 X(80) *
							String ordering_customer_party_name = strQueue.popBytesDiffString(80);
							data.setOrdering_customer_party_name(ordering_customer_party_name);
							if (ETL_Tool_FormatCheck.isEmpty(ordering_customer_party_name)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯款人顧客姓名", "空值"));
							}

							// 匯款人顧客地址 X(100)
							String ordering_customer_address_line = strQueue.popBytesString(100);
							data.setOrdering_customer_address_line(ordering_customer_address_line);

							// 匯款銀行BIC 編碼 X(20) * 預設 N/A
							String ordering_bank_bic = strQueue.popBytesString(20);
							data.setOrdering_bank_bic(ordering_bank_bic);
							if (ETL_Tool_FormatCheck.isEmpty(ordering_bank_bic)) {

								data.setOrdering_customer_account_number("N/A");
								transDataErrorMark(data, "W");

								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "匯款銀行BIC 編碼", "空值");
								errorLog_Data.setDEFAULT_VALUE("N/A");
								errorList.add(errorLog_Data);

							} else {
								if (("N/A".equals(ordering_bank_bic.trim()))) {

								} else if (ordering_bank_bic.length() < 6) {

									data.setOrdering_customer_account_number("N/A");
									transDataErrorMark(data, "W");

									errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "匯款銀行BIC 編碼", "長度小於6:" + ordering_bank_bic);
									errorLog_Data.setDEFAULT_VALUE("N/A");
									errorList.add(errorLog_Data);

								} else if (!checkMaps.get("BIC")
										.containsKey(getCOMM_NATIONALITY_CODE(ordering_bank_bic.trim()))) {

									data.setOrdering_customer_account_number("N/A");
									transDataErrorMark(data, "W");

									errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "匯款銀行BIC 編碼", "非預期:" + ordering_bank_bic);
									errorLog_Data.setDEFAULT_VALUE("N/A");
									errorList.add(errorLog_Data);
	
								}
							}

							// 受款人銀行帳戶編號 X(50) * 預設 N/A
							String beneficiary_customer_account_number = strQueue.popBytesString(50);
							data.setBeneficiary_customer_account_number(beneficiary_customer_account_number);
							if (ETL_Tool_FormatCheck.isEmpty(beneficiary_customer_account_number)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "受款人銀行帳戶編號", "空值"));
							}

							// 受款人姓名 X(80) *
							String beneficiary_customer_party_name = strQueue.popBytesDiffString(80);
							data.setBeneficiary_customer_party_name(beneficiary_customer_party_name);
							if (ETL_Tool_FormatCheck.isEmpty(beneficiary_customer_party_name)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "受款人姓名", "空值"));
							}

							// 受款銀行顧客編號 X(50)
							String beneficiary_customer_party_id = strQueue.popBytesString(50);
							data.setBeneficiary_customer_party_id(beneficiary_customer_party_id);

							// 受款銀行BIC 編碼 X(20) * 預設 N/A
							String beneficiary_bank_bic = strQueue.popBytesString(20);
							data.setBeneficiary_bank_bic(beneficiary_bank_bic);
							if (ETL_Tool_FormatCheck.isEmpty(beneficiary_bank_bic)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "受款銀行BIC 編碼", "空值"));
							} else {
								if (("N/A".equals(beneficiary_bank_bic.trim()))) {

								} else if (beneficiary_bank_bic.length() < 6) {								
									
									data.setBeneficiary_bank_bic("N/A");
									transDataErrorMark(data, "W");

									errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(beneficiary_bank_bic), "受款銀行BIC 編碼",
											"長度小於6:" + beneficiary_bank_bic);
									errorLog_Data.setDEFAULT_VALUE("N/A");
									errorList.add(errorLog_Data);

								} else if (!checkMaps.get("BIC")
										.containsKey(getCOMM_NATIONALITY_CODE(beneficiary_bank_bic.trim()))) {

									data.setBeneficiary_bank_bic("N/A");
									transDataErrorMark(data, "W");

									errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "受款銀行BIC 編碼", "非預期:" + beneficiary_bank_bic);
									errorLog_Data.setDEFAULT_VALUE("N/A");
									errorList.add(errorLog_Data);

								}
							}

							// 交易類別 X(04) *
							String transaction_type = strQueue.popBytesString(4);
							data.setTransaction_type(transaction_type);
							if (ETL_Tool_FormatCheck.isEmpty(transaction_type)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "交易類別", "空值"));
							}

							// 匯款系統 X(10)
							String transaction_system = strQueue.popBytesString(10);
							data.setTransaction_system(transaction_system);

							// 匯款管道類別 X(10)
							String channel_type = strQueue.popBytesString(10);
							data.setChannel_type(channel_type);

							// 匯款管道編號 X(20)
							String channel_id = strQueue.popBytesString(20);
							data.setChannel_id(channel_id);

							// 匯款執行分行 X(20)
							String execution_branch_code = strQueue.popBytesString(20);
							data.setExecution_branch_code(execution_branch_code);

							// 匯款執行者代號 X(50)
							String executer_id = strQueue.popBytesString(50);
							data.setExecuter_id(executer_id);

							// TODO V11
							// ErrorList統一寫入PK值, 再寫入Error Log
							String tempSRC_Data = "";
							for (int j = 0; j < errorList.size(); j++) {
								// 寫入pk值
								errorList.get(j).setDOMAIN_ID(domain_id);
								errorList.get(j).setPARTY_NUMBER(party_number);
								errorList.get(j).setTRANSACTION_ID(transfer_id);

								if (j == 0) {
									//tempSRC_Data = strQueue.getAllString(); // 無難字轉換
									tempSRC_Data = strQueue.getAllDiffString(); // 有難字轉換
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
							} else if ("W".equals(data.getError_mark())) { // TODO V11 新增一個else if, 在 if 與else之間
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
						}
					}

					insert_TRANSFER_TEMP_Datas();

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

						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 700 = 723)
						if (strQueue.getTotalByteLength() != 723) {
							fileFmtErrMsg = "尾錄位元數非預期723:" + strQueue.getTotalByteLength();
							;
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

						// 保留欄檢核(700)
						String keepColumn = strQueue.popBytesString(700);

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

					// TODO V6 END
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
							(successCount + warningCount + failureCount), successCount, warningCount, failureCount, file_exe_result,
							file_exe_result_description);
				} catch (Exception ex) {
					// 發生錯誤時, 資料List & 計數 reset // TODO V6_2
					this.dataCount = 0;
					this.dataList.clear();

					// 寫入Error_Log
					ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
							"E", "0", "ETL_E_TRANSFER程式處理", ex.getMessage(), null); // TODO V4 NEW

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
						"0", "ETL_E_TRANSFER程式處理", detail_exe_result_description, null); // TODO V4 NEW

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
					"0", "ETL_E_TRANSFER程式處理", ex.getMessage(), null); // TODO V4 NEW

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
		}

		System.out.println("#######Extrace - ETL_E_TRANSFER - End"); //

	}

	private static String getCOMM_NATIONALITY_CODE(String str) {
		if (str == null || str.length() < 6) {
			return null;
		}

		return str.substring(4, 6);
	}

	// List增加一個data
	private void addData(ETL_Bean_TRANSFER_TEMP_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_TRANSFER_TEMP_Datas();
		}
	}

	private void insert_TRANSFER_TEMP_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_TRANSFER - insert_TRANSFER_TEMP_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_TRANSFER_TEMP(?,?)}"); // 呼叫寫入DB2 - SP
		insertAdapter.setCreateArrayTypesName("A_TRANSFER_TEMP"); // DB2 array type
		insertAdapter.setCreateStructTypeName("T_TRANSFER_TEMP"); // DB2 type
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.dataList, insertAdapter); // TODO V6_2
		int errorCount = insertAdapter.getErrorCount(); // TODO V6_2

		if (isSuccess) {
			System.out.println("insert_TRANSFER_Datas 寫入 " + this.dataList.size() + "(-" + errorCount + ")筆資料!"); // TODO V6_2
			this.oneFileInsertErrorCount = this.oneFileInsertErrorCount + errorCount; // TODO V6_2
		} else {
			throw new Exception("insert_TRANSFER_Datas 發生錯誤");
		}

		this.dataCount = 0;
		this.dataList.clear();
	}
	
	// 轉換Error_Mark
	private static void transDataErrorMark(ETL_Bean_TRANSFER_TEMP_Data data, String trans_Error_Mark) {
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
		ETL_E_TRANSFER program = new ETL_E_TRANSFER();
		String filePath = "D:\\company\\pershing\\agribank\\test_data\\20181025";
		String fileTypeName = "TRANSFER";
		String batch_no = "test18";
		String exc_central_no = "018";
		String exc_record_date = "20181024";
		String upload_no = "001";
		String program_no = "aaa";
		program.read_Transfer_File(filePath, fileTypeName, batch_no, exc_central_no,
				new SimpleDateFormat("yyyyMMdd").parse(exc_record_date), upload_no, program_no);

	
	}

}
