package Extract;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_PARTY_PHONE_Data;
import Bean.ETL_Bean_SPARTY_Data;
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

public class ETL_E_SPARTY extends Extract {
	
	public ETL_E_SPARTY() {
		
	}

	public ETL_E_SPARTY(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) {
		super(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
	}

	@Override
	public void read_File() {
		try {
			read_Sparty_File(this.filePath, this.fileTypeName, this.batch_no, this.exc_central_no,
					this.exc_record_date, this.upload_no, this.program_no);
		} catch (Exception e) {
			e.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			System.out.println("ExceptionMassage:"+sw.toString());
	
		} 
	}

	// 進階檢核參數
	private boolean advancedCheck = ETL_Profile.AdvancedCheck;

	// 欄位檢核用陣列
	private String[][] checkMapArray = {
			{ "scustbox_file_type", "SCUSTBOX_FILE_TYPE" }, // 業務別
			{ "comm_domain_id", "COMM_DOMAIN_ID" }, // 本會代號
			{ "s-4", "PARTY_CHANGE_CODE" }, // 異動代號
			{ "country", "COMM_NATIONALITY_CODE" }, // 客戶國籍
			{ "s-8", "PARTY_ENTITY_TYPE" }, //客戶類型
			{ "s9-1", "COMM_OCCUPATION_CODE3" }, // 職業/行業
			{ "s9-2", "COMM_OCCUPATION_CODE6" } // 職業/行業
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
	private List<ETL_Bean_SPARTY_Data> dataList = new ArrayList<ETL_Bean_SPARTY_Data>();

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
	public void read_Sparty_File(String filePath, String fileTypeName, String batch_no, String exc_central_no,
			Date exc_record_date, String upload_no, String program_no) throws Exception {

		// 取得所有檢核用子map, 置入母map內
		try {

			checkMaps = new ETL_Q_ColumnCheckCodes().getCheckMaps(exc_record_date, exc_central_no, checkMapArray);
		} catch (Exception ex) {
			checkMaps = null;
			System.out.println("ETL_E_SPARTY 抓取checkMaps資料有誤!"); // TODO  V6_2
			ex.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:"+sw.toString());

		}

		System.out.println("#######Extrace - ETL_E_SPARTY - Start");

		try {
			// 批次不重複執行
			if (ETL_P_Log.query_ETL_Detail_Log_Done(batch_no, exc_central_no, exc_record_date, upload_no, "E",
					program_no)) {
				String inforMation = "batch_no = " + batch_no + ", " + "exc_central_no = " + exc_central_no + ", "
						+ "exc_record_date = " + exc_record_date + ", " + "upload_no = " + upload_no + ", "
						+ "step_type = E, " + "program_no = " + program_no;

				System.out.println("#######Extrace - ETL_E_SPARTY - 不重複執行\n" + inforMation); 
				System.out.println("#######Extrace - ETL_E_SPARTY - End"); 

				return;
			}

			// 處理前寫入ETL_Detail_Log
			ETL_P_Log.write_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no, "S",
					"", "", new Date(), null);

			// 處理SPARTY錯誤計數
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
						ETL_E_SPARTY.class);
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
					System.out.println("## ETL_E_SPARTY - read_SPARTY_File - 控制程式無提供報送單位，不進行解析！");
					processErrMsg = processErrMsg + "控制程式無提供報送單位，不進行解析！\n";
					continue;
				} else if (!exc_central_no.trim().equals(pfn.getCentral_No().trim())) {
					System.out.println("##" + pfn.getFileName() + " 處理報送單位非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理報送單位非預期，不進行解析！\n";
					continue;
				}

				// 業務別非預期, 不進行解析
				if (pfn.getFile_Type() == null || "".equals(pfn.getFile_Type().trim())
						|| !checkMaps.get("scustbox_file_type").containsKey(pfn.getFile_Type().trim())) {

					System.out.println("##" + pfn.getFileName() + " 處理業務別非預期，不進行解析！");
					processErrMsg = processErrMsg + pfn.getFileName() + " 處理業務別非預期，不進行解析！\n";
					continue;
				}

				// 資料日期非預期, 不進行解析
				if (exc_record_date == null) {
					System.out.println("## ETL_E_SPARTY - read_SPARTY_File - 控制程式無提供資料日期，不進行解析！");
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

				try {

					// TODO V11 (多一個0, 在0, 之間)
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
					fileFmtErrMsg = isFileFormatOK ? "":"區別碼錯誤";
					

					// 首錄檢查
					if (isFileFormatOK) {

						// 注入指定範圍筆數資料到QUEUE
						strQueue.setBytesList(fileByteUtil.getFilesBytes());

						// strQueue工具注入第一筆資料
						strQueue.setTargetString();

						// 檢查整行bytes數(1 + 7 + 8 + 343 = 359)
						if (strQueue.getTotalByteLength() != 359) {
							fileFmtErrMsg = "首錄位元數非預期359:" + strQueue.getTotalByteLength();
							
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別瑪檢核(1)
						String typeCode = strQueue.popBytesString(1);
						if (!"1".equals(typeCode)) { // 首錄區別碼檢查, 嚴重錯誤,
														// 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查,
																		// 嚴重錯誤,
																		// 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "首錄報送單位代碼與檔名不符:" + central_no; // TODO
																			// V4
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
							fileFmtErrMsg = "首錄檔案日期與檔名不符:" + record_date; // TODO
																			// V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "首錄檔案日期格式錯誤:" + record_date; // TODO
																			// V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 保留欄位檢核(343)
						String keepColumn = strQueue.popBytesString(343);

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
							ETL_Bean_SPARTY_Data data = new ETL_Bean_SPARTY_Data(pfn);
							// 寫入資料行數
							data.setRow_count(rowCount);

	
							// 整行bytes數檢核(1 + 7 + 11 + 1 + 80 + 8 + 2 + 3 + 6 + 20 + 20 + 100 + 100= 359)
							if (strQueue.getTotalByteLength() != 359) {
								data.setError_mark("Y");
								errWriter.addErrLog(
										new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
												"行數bytes檢查", "非預期359:" + strQueue.getTotalByteLength()));

								// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
								failureCount++;
								rowCount++;
								grandTotal++; //TODO V6
								continue;
							}
							
							// TODO V11  寫入ErrorList
							List<ETL_Bean_ErrorLog_Data> errorList = new ArrayList<ETL_Bean_ErrorLog_Data>();

							// 區別碼檢核 s-1*
							String typeCode = strQueue.popBytesString(1); // TODO
																			// V4
							if (!"2".equals(typeCode)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "區別碼", "非預期:" + typeCode));
							}

							// 本會代號檢核(7) s-2*
							String domain_id = strQueue.popBytesString(7);
							data.setDomain_id(domain_id);
							if (ETL_Tool_FormatCheck.isEmpty(domain_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "空值"));
							} else if (advancedCheck && !checkMaps.get("comm_domain_id").containsKey(domain_id)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "本會代號", "非預期:" + domain_id));
							}

							// 客戶統編檢核(11) s-3*
							String party_number = strQueue.popBytesString(11);
							data.setParty_number(party_number);
							if (ETL_Tool_FormatCheck.isEmpty(party_number)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶統編", "空值"));
							}

							// 異動代號檢核(1) s-4*
							String change_code = strQueue.popBytesString(1);
							data.setChange_code(change_code);
							if (ETL_Tool_FormatCheck.isEmpty(change_code)) {
//								data.setError_mark("Y");
								// 預設值 - U
								data.setChange_code("U");
								transDataErrorMark(data, "W");
								
								errorLog_Data = new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "異動代號", "空值");
								errorLog_Data.setDEFAULT_VALUE("U");
								errorList.add(errorLog_Data);

							} else if (advancedCheck && !checkMaps.get("s-4").containsKey(change_code)) {
//								data.setError_mark("Y");
								// 預設值 - U
								data.setChange_code("U");
								transDataErrorMark(data, "W");
								
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "異動代號", "非預期:" + change_code);
								errorLog_Data.setDEFAULT_VALUE("U");
								errorList.add(errorLog_Data);
						
							}



							// 客戶姓名(80) s-5*
							String party_last_name_1 = strQueue.popBytesDiffString(80);
							data.setParty_last_name_1(party_last_name_1);
							if (ETL_Tool_FormatCheck.isEmpty(party_last_name_1)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶姓名", "空值"));
							}
							
							// 客戶生日(8) s-6*
							String date_of_birth = strQueue.popBytesString(8);
							data.setDate_of_birth(ETL_Tool_StringX.toUtilDate(date_of_birth));
							if (advancedCheck && !ETL_Tool_FormatCheck.isEmpty(date_of_birth)
									&& !ETL_Tool_FormatCheck.checkDate(date_of_birth)) {
//								data.setError_mark("Y");
								// 預設值 - 1900-01-01
								data.setDate_of_birth(ETL_Tool_StringX.toUtilDate("19000101"));
								transDataErrorMark(data, "W");
								
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶生日", "日期格式不正確:" + date_of_birth);
								errorLog_Data.setDEFAULT_VALUE("1900-01-01");
								errorList.add(errorLog_Data);
								
							}else if (ETL_Tool_FormatCheck.isEmpty(date_of_birth)) {
//								data.setError_mark("Y");
								// 預設值 - 1900-01-01
								data.setDate_of_birth(ETL_Tool_StringX.toUtilDate("19000101"));
								transDataErrorMark(data, "W");
								
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶生日", "空值");
								errorLog_Data.setDEFAULT_VALUE("1900-01-01");
								errorList.add(errorLog_Data);
							
							}
							
							// 客戶國籍 s-7*
							String nationality_code = strQueue.popBytesString(2);
							data.setNationality_code(nationality_code);
							
							if (!checkMaps.get("country").containsKey(nationality_code)) {
//								data.setError_mark("Y");
								// 預設值 - TW
								data.setNationality_code("TW");
								transDataErrorMark(data, "W");
								
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶國籍", "非預期:" + nationality_code);
								errorLog_Data.setDEFAULT_VALUE("TW");
								errorList.add(errorLog_Data);
		
							} else if ("　".equals(nationality_code)) { // 國籍代碼防全形空白錯誤  2018.03.28
//								data.setNationality_code("  ");
//								data.setError_mark("Y");
								// 預設值 - TW
								data.setNationality_code("TW");
								transDataErrorMark(data, "W");
								
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶國籍", "全形空白錯誤");
								errorLog_Data.setDEFAULT_VALUE("TW");
								errorList.add(errorLog_Data);
						
							}else if (ETL_Tool_FormatCheck.isEmpty(nationality_code)) {
//								data.setError_mark("Y");
								// 預設值 - TW
								data.setNationality_code("TW");
								transDataErrorMark(data, "W");
								
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶國籍", "空值");
								errorLog_Data.setDEFAULT_VALUE("TW");
								errorList.add(errorLog_Data);
				
							}
							
							// 顧客類型 s-8*(3)
							String entity_type = strQueue.popBytesString(3);
							data.setEntity_type(entity_type);
							if (ETL_Tool_FormatCheck.isEmpty(entity_type)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "顧客類型", "空值"));
							} else if (!checkMaps.get("s-8").containsKey(entity_type)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "顧客類型", "非預期:" + entity_type));
							}
							
							// 客戶職業/行業 s-9*(6)
							String occupation_code = strQueue.popBytesString(6);
							
							if (ETL_Tool_FormatCheck.isEmpty(occupation_code)) {
//								data.setError_mark("Y");
								// 預設值 - 自然人：211 ； 法人：N/A
								if ("100".equals(entity_type)) {
									data.setOccupation_code("211");									
								} else {
									data.setOccupation_code("N/A");
								}
							
								transDataErrorMark(data, "W");
								
								errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶職業/行業", "空值");
								if ("100".equals(entity_type)) {
									errorLog_Data.setDEFAULT_VALUE("211");
								} else {
									errorLog_Data.setDEFAULT_VALUE("N/A");
								}
								errorList.add(errorLog_Data);
			
							} else {
								if (advancedCheck && checkMaps.get("s9-1").containsKey(occupation_code.trim())) { 
									data.setOccupation_code(occupation_code);
								} else if (advancedCheck && checkMaps.get("s9-2").containsKey(occupation_code.trim())) {
									data.setOccupation_code(occupation_code);
								}else
								{
//									data.setError_mark("Y");
									
									// 預設值 - 自然人：211 ； 法人：N/A
									if ("100".equals(entity_type)) {
										data.setOccupation_code("211");									
									} else {
										data.setOccupation_code("N/A");
									}
								
									transDataErrorMark(data, "W");
									
									errorLog_Data =new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
											String.valueOf(rowCount), "客戶職業/行業", "非預期:" + occupation_code);
									
									if ("100".equals(entity_type)) {
										errorLog_Data.setDEFAULT_VALUE("211");
									} else {
										errorLog_Data.setDEFAULT_VALUE("N/A");
									}
									errorList.add(errorLog_Data);
				
								}
							}
							
							// 客戶手機 s-10(20)
							String cell_phone_number = strQueue.popBytesString(20);
							data.setCell_phone_number(cell_phone_number);
							
							// 客戶電話 s-11(20)
							String phone_number = strQueue.popBytesString(20);
							data.setPhone_number(phone_number);
							
							// 客戶戶籍地址 s-12(100)
							String address_line_1 = strQueue.popBytesString(100);
							data.setAddress_line_1(address_line_1);
							
							// 客戶通訊地址 s-13*(100)
							String address_line_2 = strQueue.popBytesString(100);
							data.setAddress_line_2(address_line_2);
							if (ETL_Tool_FormatCheck.isEmpty(address_line_2)) {
								data.setError_mark("Y");
								errorList.add(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
										String.valueOf(rowCount), "客戶通訊地址", "空值"));
							}
							
							// TODO V11
							// ErrorList統一寫入PK值, 再寫入Error Log
							String tempSRC_Data = "";
							for (int j = 0; j < errorList.size(); j++) {
								// 寫入pk值
								errorList.get(j).setDOMAIN_ID(domain_id);
								errorList.get(j).setPARTY_NUMBER(party_number);
								
								// 第一筆時，將轉換String預存
								if (j == 0) {
//									tempSRC_Data = strQueue.getAllString(); // 無難字轉換
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
							} else if ("W".equals(data.getError_mark())) { // TODO V11  新增一個else if, 在 if 與else之間
								// 若參數為"W" 計入warning Count
								warningCount++;
								// 將Error_Mark改為"", 視為正常繼續ETL作業
								transDataErrorMark(data, "");
							} else {
								successCount++;
							}
							
							// 實際處理明細錄筆數累加
							grandTotal += 1;


							rowCount++; // 處理行數 + 1

							/*
							 * 第一個條件是 初次處理，且資料總筆數比制定範圍大時 會進入條件
							 * 第二個條件是非初次處理，且個別資料來源已處理的筆數，可以被制定範圍整除時進入
							 */
							if ((isFirstTime && (isFileOK >= ETL_Profile.ETL_E_Stage)
									&& grandTotal == (ETL_Profile.ETL_E_Stage - 1))
									|| (!isFirstTime && (strQueue.getBytesListIndex() % ETL_Profile.ETL_E_Stage == 0))

							) {

								//記錄非初次
								isFirstTime = false;


								// 注入指定範圍筆數資料到QUEUE
								strQueue.setBytesList(fileByteUtil.getFilesBytes());
								// 初始化使用筆數
								strQueue.setBytesListIndex(0);

							}
							
						}
					}

					System.out.println("this.dataList size = " + this.dataList.size());
					
					// Sparty_Data寫入DB
					insert_Sparty_Datas();
					
					
					// 修正筆數, 考慮寫入資料庫時寫入失敗的狀況
					successCount = successCount - this.oneFileInsertErrorCount;
					failureCount = failureCount + this.oneFileInsertErrorCount;
					// 單一檔案寫入DB error個數重計
					this.oneFileInsertErrorCount = 0;				

					// 尾錄檢查
					if (isFileFormatOK && "".equals(fileFmtErrMsg)) { // 沒有嚴重錯誤時進行

						strQueue.setTargetString();
						

						// 整行bytes數檢核 (1 + 7 + 8 + 7 + 336 = 359)
						if (strQueue.getTotalByteLength() != 359) {
							fileFmtErrMsg = "尾錄位元數非預期359:" + strQueue.getTotalByteLength(); // TODO
																							// V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", fileFmtErrMsg));
						}

						// 區別碼檢核(1) // TODO V4 Start
						String typeCode = strQueue.popBytesString(1);
						if (!"3".equals(typeCode)) {
							fileFmtErrMsg = "尾錄區別碼有誤:" + typeCode;
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", fileFmtErrMsg));
						}
						// TODO V4 End

						// 報送單位檢核(7)
						String central_no = strQueue.popBytesString(7);
						if (!central_no.equals(pfn.getCentral_No())) { // 報送單位一致性檢查,
																		// 嚴重錯誤,
																		// 不進行迴圈並記錄錯誤訊息
							fileFmtErrMsg = "尾錄報送單位代碼與檔名不符:" + central_no; // TODO
																			// V4
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
							fileFmtErrMsg = "尾錄檔案日期與檔名不符:" + record_date; // TODO
																			// V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
							fileFmtErrMsg = "尾錄檔案日期格式錯誤:" + record_date; // TODO
																			// V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "檔案日期", fileFmtErrMsg));
						}

						// 總筆數檢核(7)
						String totalCount = strQueue.popBytesString(7);
						// iTotalCount = ETL_Tool_StringX.toInt(totalCount); TODO V5

						if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
							fileFmtErrMsg = "尾錄總筆數格式錯誤:" + totalCount; // TODO
																		// V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
							fileFmtErrMsg = "尾錄總筆數與統計不符:" + totalCount + " != " + (rowCount - 2); // TODO
																									// V4
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "總筆數", fileFmtErrMsg));
						}

						// 保留欄檢核(336)
						String keepColumn = strQueue.popBytesString(336);
						
						// TODO V11  增加WarningCount
						// 程式統計檢核
						if ((rowCount - 2) != (successCount + warningCount + failureCount)) {
							fileFmtErrMsg = "總筆數 <> 成功比數 + 警示筆數 + 失敗筆數";
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

					// TODO V11    增加warningCount
					// 處理後更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", parseEndDate,
							(successCount + warningCount + failureCount),
							successCount, warningCount, failureCount, file_exe_result, file_exe_result_description);
				} catch (Exception ex) {
					// 發生錯誤時, 資料List & 計數 reset 
					this.dataCount = 0; 
					this.dataList.clear();

					// 寫入Error_Log
					ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
							"E", "0", "ETL_E_SPART程式處理", ex.getMessage(), null); // TODO V4 NEW

					// 執行錯誤更新ETL_FILE_Log
					ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), pfn.getCentral_No(), exc_record_date,
							pfn.getFile_Type(), pfn.getFile_Name(), upload_no, "E", new Date(), 0, 0, 0, 0, "S",
							ex.getMessage()); // TODO V4 (0, 0, 0)<=(iTotalCount, successCount, failureCount)
					processErrMsg = processErrMsg + ex.getMessage() + "\n";

					ex.printStackTrace();
					StringWriter sw = new StringWriter();
					PrintWriter pw = new PrintWriter(sw);
					ex.printStackTrace(pw);
					System.out.println("ExceptionMassage:"+sw.toString());

				}

				// 累加SPART處理錯誤筆數
				detail_ErrorCount = detail_ErrorCount + failureCount;
			}

			// 過濾軌跡資料
			try {
				
				ETL_P_EData_Filter.E_Datas_Filter("filter_SPARTY_Temp_Temp", // TODO v6_2
						batch_no, exc_central_no, exc_record_date, upload_no, program_no);
				
			} catch (Exception ex) {
				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no,
						"E", "0", "ETL_E_SPARTY程式處理", ex.getMessage(), null); // TODO v6_2
				processErrMsg = processErrMsg + ex.getMessage() + "\n";
				
				ex.printStackTrace();
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				ex.printStackTrace(pw);
				System.out.println("ExceptionMassage:"+sw.toString());

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
						"0", "ETL_E_SPARTY程式處理", detail_exe_result_description, null); 

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
			// TODO V4 NEW
			// 寫入Error_Log
			ETL_P_Log.write_Error_Log(batch_no, exc_central_no, exc_record_date, null, fileTypeName, upload_no, "E",
					"0", "ETL_E_SPARTY程式處理", ex.getMessage(), null); 

			// 處理後更新ETL_Detail_Log
			ETL_P_Log.update_End_ETL_Detail_Log(batch_no, exc_central_no, exc_record_date, upload_no, "E", program_no,
					"E", "S", ex.getMessage(), new Date());

			ex.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:"+sw.toString());

		}

		System.out.println("#######Extrace - ETL_E_SPARTY - End");
	}

	// List增加一個data
	private void addData(ETL_Bean_SPARTY_Data data) throws Exception {
		this.dataList.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_Sparty_Datas();
		}
	}

	// 將SPARTY資料寫入資料庫
	private void insert_Sparty_Datas() throws Exception {
		if (this.dataList == null || this.dataList.size() == 0) {
			System.out.println("ETL_E_SPARTY - insert_Sparty_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		insertAdapter.setSql("{call SP_INSERT_SPARTY_TEMP(?,?)}"); // 呼叫SPARTY寫入DB2 - SP  
		insertAdapter.setCreateArrayTypesName("A_SPARTY"); // DB2 array type - SPARTY
		insertAdapter.setCreateStructTypeName("T_SPARTY"); // DB2 type - SPARTY
		insertAdapter.setTypeArrayLength(ETL_Profile.Data_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.dataList, insertAdapter); 
		int errorCount = insertAdapter.getErrorCount(); 

		if (isSuccess) {
			System.out.println("insert_Sparty_Datas 寫入 " + this.dataList.size() + "(-" + errorCount + ")筆資料!"); 
			this.oneFileInsertErrorCount = this.oneFileInsertErrorCount + errorCount; 
		} else {
			throw new Exception("insert_Sparty_Datas 發生錯誤");
		}

		// TODO V4
		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.dataList.clear();
	}
	// TODO V11
	// 轉換Error_Mark
	private static void transDataErrorMark(ETL_Bean_SPARTY_Data data, String trans_Error_Mark) {
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
		ETL_E_SPARTY one = new ETL_E_SPARTY();
		// String filePath = "C:/Users/10404003/Desktop/農經/2018/180205/test";
		String filePath = "C:\\Users\\Windos\\Desktop\\test";
		String fileTypeName = "SPARTY";
		one.read_Sparty_File(filePath, fileTypeName,
				"test0037", "600", new SimpleDateFormat("yyyyMMdd").parse("20180705"), "004", "ETL_E_SPARTY");
	}

}
