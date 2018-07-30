package Migration;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import Bean.ETL_Bean_DM_BRANCHMAPPING_LOAD_Data;
import Bean.ETL_Bean_ErrorLog_Data;
import Bean.ETL_Bean_Response;
import DB.ETL_P_DMData_Writer;
import DB.ETL_P_Data_Writer;
import DB.ETL_P_ErrorLog_Writer;
import DB.ETL_P_Log;
import DB.InsertAdapter;
import Profile.ETL_Profile;
import Tool.ETL_Tool_DM_ParseFileName;
import Tool.ETL_Tool_FileByteUtil;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_StringQueue;
import Tool.ETL_Tool_StringX;

public class ETL_DM_BRANCHMAPPING_LOAD {

	// data寫入域值
	private int stageLimit = ETL_Profile.Data_Stage;

	// list data筆數
	private int dataCount = 0;

	private int oneFileInsertErrorCount = 0;

	// Data儲存List
	private List<ETL_Bean_DM_BRANCHMAPPING_LOAD_Data> datas = new ArrayList<ETL_Bean_DM_BRANCHMAPPING_LOAD_Data>();
	
	
	public void read_DM_BRANCHMAPPING_LOAD_File(String hostName, String port, String username, String password,
			String directory, String savePath, String batch_no, String exc_central_no, String fileTypeName, Date targetDate) throws Exception {
		
		System.out.println("read_DM_BRANCHMAPPING_LOAD_File Start");
		String upload_no = "";

		// 程式執行錯誤訊息
		String processErrMsg = "";

		// 取得目標檔案File
		List<File> fileList = ETL_Tool_FileReader.getTRTargetFileList(hostName, port, username, password, directory,
				savePath, fileTypeName, targetDate);



		System.out.println("共有檔案 " + fileList.size() + " 個！");
		System.out.println("===============");
		for (int i = 0; i < fileList.size(); i++) {
			System.out.println(fileList.get(i).getName());
		}
		System.out.println("===============");

		// 如果有符合規格的資料就清空五代
		if (fileList.size() > 0) {
			// 抓取第一筆
			File parseFile = fileList.get(0);

			// 檔名
			String fileName = parseFile.getName();

			// 解析fileName物件
			ETL_Tool_DM_ParseFileName pfn = new ETL_Tool_DM_ParseFileName(fileName);

			// (1) 若目的檔(ACCTMAPPING)有資料則清空
			ETL_P_DMData_Writer.truncateMappingDataTable("BRANCHMAPPING", exc_central_no);

		} else {
			// "file_log 無檔案 狀態S";

			
			// 開始前ETL_FILE_Log寫入DB
			ETL_P_Log.write_ETL_FILE_Log(batch_no, exc_central_no, targetDate, "", "BRANCHMAPPING",
					upload_no, "E", new Date(), null, 0, 0, 0, "BRANCHMAPPING");

			// 執行錯誤更新ETL_FILE_Log
			ETL_P_Log.update_End_ETL_FILE_Log(batch_no, exc_central_no, targetDate, "",
					"BRANCHMAPPING", upload_no, "E", new Date(), 0, 0, 0, "S", "無該日期之檔案");

			System.out.println("無該日期之檔案！");
		}

		// 進行檔案處理
		for (int i = 0; i < fileList.size(); i++) {
			// 開始執行時間
			Date parseStartDate = new Date();

			// 取得檔案
			File parseFile = fileList.get(i);

			ETL_Tool_FileByteUtil fileByteUtil = new ETL_Tool_FileByteUtil(parseFile.getAbsolutePath(),
					ETL_DM_BRANCHMAPPING_LOAD.class);

			// 檔名
			String fileName = parseFile.getName();

			// 解析fileName物件
			ETL_Tool_DM_ParseFileName pfn = new ETL_Tool_DM_ParseFileName(fileName);

			// 設定批次編號
			pfn.setBatch_no(batch_no);

			// rowCount == 處理行數
			int rowCount = 1; // 從1開始
			// 成功計數
			int successCount = 0;
			// 失敗計數
			int failureCount = 0;
			// 紀錄是否第一次
			boolean isFirstTime = false;

			try {

				// 開始前ETL_FILE_Log寫入DB
				ETL_P_Log.write_ETL_FILE_Log(pfn.getBatch_no(), exc_central_no, targetDate, "", pfn.getFile_name(),
						upload_no, "E", parseStartDate, null, 0, 0, 0, pfn.getFileName());

				// 嚴重錯誤訊息變數(讀檔)
				String fileFmtErrMsg = "";

				// ETL_字串處理Queue
				ETL_Tool_StringQueue strQueue = new ETL_Tool_StringQueue(exc_central_no);

				// ETL_Error Log寫入輔助工具
				ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();

				// 首、明細、尾錄, 基本組成檢查
				int isFileOK = fileByteUtil.isDMFileOK(pfn, upload_no, parseFile.getAbsolutePath());

				boolean isFileFormatOK = isFileOK != 0 ? true : false;

				fileFmtErrMsg = isFileFormatOK ? "" : "區別碼錯誤";

				// 首錄檢查
				if (isFileFormatOK) {

					// 注入指定範圍筆數資料到QUEUE
					strQueue.setBytesList(fileByteUtil.getFilesBytes());

					// strQueue工具注入第一筆資料
					strQueue.setTargetString();

					// 檢查整行bytes數(1 + 7 + 8 + 9 = 25)
					if (strQueue.getTotalByteLength() != 25) {
						fileFmtErrMsg = "首錄位元數非預期25:" + strQueue.getTotalByteLength();
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"行數bytes檢查", fileFmtErrMsg));
					}

					// 區別瑪檢核(1)
					String typeCode = strQueue.popBytesString(1);

					// 首錄區別碼檢查, 嚴重錯誤,不進行迴圈並記錄錯誤訊息
					if (!"1".equals(typeCode)) {
						fileFmtErrMsg = "首錄區別碼有誤:" + typeCode;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"區別碼", fileFmtErrMsg));
					}

					/*
					 * 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
					 */
					String central_no = strQueue.popBytesString(7);
					if (!central_no.trim().equals(exc_central_no)) {
						fileFmtErrMsg = "首錄報送單位代碼與檔名不符:" + central_no;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"報送單位", fileFmtErrMsg));
					}

					// 檔案日期檢核(8)
					String record_date = strQueue.popBytesString(8);
					if (ETL_Tool_FormatCheck.isEmpty(record_date)) {
						fileFmtErrMsg = "首錄檔案日期空值";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!record_date.equals(pfn.getRecord_date_str())) {
						fileFmtErrMsg = "首錄檔案日期與檔名不符:" + record_date;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
						fileFmtErrMsg = "首錄檔案日期格式錯誤:" + record_date;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					}

					// 保留欄檢核(9)
					String reserve_field = strQueue.popBytesString(9);

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
						ETL_Bean_DM_BRANCHMAPPING_LOAD_Data data = new ETL_Bean_DM_BRANCHMAPPING_LOAD_Data(pfn);

						data.setRow_count(rowCount);
						/*
						 * 整行bytes數檢核(01+ 07+ 07+ 10 = 25)
						 */
						if (strQueue.getTotalByteLength() != 25) {
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "行數bytes檢查", "非預期25:" + strQueue.getTotalByteLength()));

							// 明細錄資料bytes不正確, 跳過此行後續檢核, 執行下一行
							rowCount++;
							grandTotal++;
							continue;
						}

						// 區別碼檢核 R X(01)*
						String typeCode = strQueue.popBytesString(1);
						if (!"2".equals(typeCode)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "區別碼", "非預期:" + typeCode));
						}

						// 原分支機構代碼 R X(07)*
						String old_branch_code = strQueue.popBytesString(7);
						data.setOld_branch_code(old_branch_code);

						if (ETL_Tool_FormatCheck.isEmpty(old_branch_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "原分支機構代碼", "空值"));
						}

						// 新分支機構代號 R X(07)*
						String new_branch_code = strQueue.popBytesString(7);
						data.setNew_branch_code(new_branch_code);

						if (ETL_Tool_FormatCheck.isEmpty(new_branch_code)) {
							data.setError_mark("Y");
							errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E",
									String.valueOf(rowCount), "新分支機構代碼", "空值"));
						}

						// 保留欄 R X(10)*
						strQueue.popBytesString(10);

						// data list 加入一個檔案
						addData(data);

						grandTotal += 1;// 實際處理明細錄筆數累加

						rowCount++; // 處理行數 + 1

						/*
						 * 第一個條件是 初次處理，且資料總筆數比制定範圍大時 會進入條件 第二個條件是非初次處理，且個別資料來源已處理的筆數，可以被制定範圍整除時進入
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
				// BRANCHMAPPING_Data寫入DB
				insert_BRANCHMAPPING_LOAD_Datas();

				// 更新PK重複的ROW比較大的 ERROR_MARK='Y'
				// 取得 ERROR是''的 為正確數量 扣掉 全部的數量 就是錯誤的數量
				// 修正筆數, 考慮寫入資料庫時寫入失敗的狀況 當無法查到資料直接跳出
				ETL_Bean_Response obj = ETL_P_DMData_Writer.checkBranchmappingData(exc_central_no, fileName);
				Integer dataStatus = 0;

				if (obj.isSuccess()) {
					dataStatus = obj.getDataStatus();
					successCount = obj.getSuccessCount();
				}

				// 全部筆數去掉正確數量為錯誤筆數
				failureCount = grandTotal + this.oneFileInsertErrorCount - successCount;

				// 單一檔案寫入DB error個數重計
				this.oneFileInsertErrorCount = 0;

				// 總筆數檢核(7)
				String totalCount = "";

				// 尾錄檢查
				if (isFileFormatOK && "".equals(fileFmtErrMsg)) {// 沒有嚴重錯誤時進行

					strQueue.setTargetString();

					// 整行bytes數檢核 (1 + 7 + 8 + 7 + 2 = 25)
					if (strQueue.getTotalByteLength() != 25) {
						fileFmtErrMsg = "尾錄位元數非預期25:" + strQueue.getTotalByteLength();
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"行數bytes檢查", fileFmtErrMsg));
					}

					// 區別碼檢核(1)
					String typeCode = strQueue.popBytesString(1);

					if (!"3".equals(typeCode)) {
						fileFmtErrMsg = "尾錄區別碼有誤:" + typeCode;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"區別碼", fileFmtErrMsg));
					}
					/*
					 * 報送單位檢核(7) 報送單位一致性檢查,嚴重錯誤,不進行迴圈並記錄錯誤訊息
					 */
					String central_no = strQueue.popBytesString(7);

					if (!central_no.trim().equals(exc_central_no)) {
						fileFmtErrMsg = "尾錄報送單位代碼與檔名不符:" + central_no;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"報送單位", fileFmtErrMsg));
					}

					// 檔案日期檢核(8)
					String record_date = strQueue.popBytesString(8);

					if (record_date == null || "".equals(record_date.trim())) {
						fileFmtErrMsg = "尾錄檔案日期空值";
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!record_date.equals(pfn.getRecord_date_str())) {
						fileFmtErrMsg = "尾錄檔案日期與檔名不符:" + record_date;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					} else if (!ETL_Tool_FormatCheck.checkDate(record_date)) {
						fileFmtErrMsg = "尾錄檔案日期格式錯誤:" + record_date;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"檔案日期", fileFmtErrMsg));
					}

					// 總筆數檢核(7)
					totalCount = strQueue.popBytesString(7);

					if (!ETL_Tool_FormatCheck.checkNum(totalCount)) {
						fileFmtErrMsg = "尾錄總筆數格式錯誤:" + totalCount;
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"總筆數", fileFmtErrMsg));
					} else if (Integer.valueOf(totalCount) != (rowCount - 2)) {
						fileFmtErrMsg = "尾錄總筆數與統計不符:" + totalCount + "!=" + (rowCount - 2);
						errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
								"總筆數", fileFmtErrMsg));
					}
				}

				// 保留欄檢核(2)
				strQueue.popBytesString(2);

				// 程式統計檢核
				if ((rowCount - 2) != (successCount + failureCount)) {
					// PK重複處理方式
					fileFmtErrMsg = "總筆數 <> 成功比數 + 失敗筆數";
					errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(rowCount),
							"程式檢核", fileFmtErrMsg));
				}

				Date parseEndDate = new Date(); // 現在時間執行時間
				System.out.println("解析檔案： " + fileName + " End " + parseEndDate);

				// 執行結果
				String file_exe_result;
				// 執行結果說明
				String file_exe_result_description = null;

				if (!"".equals(fileFmtErrMsg)) {
					file_exe_result = "S";
					file_exe_result_description = "解析檔案出現嚴重錯誤";
					processErrMsg = processErrMsg + pfn.getFileName() + "解析檔案出現嚴重錯誤\n";
					System.out.println(fileFmtErrMsg);
				} else if (dataStatus == 1) {
					file_exe_result = "S";
					file_exe_result_description = processErrMsg + pfn.getFileName() + "舊資料重複\n";
				} else if (dataStatus == 2) {
					file_exe_result = "S";
					file_exe_result_description = processErrMsg + pfn.getFileName() + "整筆資料重複\n";
				} else if (failureCount == 0) {
					file_exe_result = "Y";
					file_exe_result_description = "執行結果無錯誤資料";
				} else {
					file_exe_result = "D";
					file_exe_result_description = "錯誤資料筆數: " + failureCount;
				}

				// Error_Log寫入DB
				errWriter.insert_Error_Log();

				// 當狀態是正常代表尾路筆數正確 可用筆數來判別是否為空檔
				if ("Y".equals(file_exe_result) && Integer.valueOf(totalCount) == 0) {
					// 如果是空檔只做程式規格一到五
					file_exe_result_description = "空檔警告:僅執行部分主檔程式 ";
				}

				ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), exc_central_no, pfn.getRecord_date(), "",
						pfn.getFile_name(), upload_no, "E", parseEndDate, (successCount + failureCount),
						successCount, failureCount, file_exe_result, file_exe_result_description);

			} catch (Exception ex) {
				// 發生錯誤時, 資料List & 計數 reset
				this.dataCount = 0;
				this.datas.clear();

				// 寫入Error_Log
				ETL_P_Log.write_Error_Log(batch_no, exc_central_no, pfn.getRecord_date(), null, fileTypeName,
						upload_no, "E", "0", "ETL_DM_BRANCHMAPPING_LOAD程式處理", ex.getMessage(), null);

				// 執行錯誤更新ETL_FILE_Log
				ETL_P_Log.update_End_ETL_FILE_Log(pfn.getBatch_no(), exc_central_no, pfn.getRecord_date(), "",
						pfn.getFile_name(), upload_no, "E", new Date(), 0, 0, 0, "S", ex.getMessage());
				processErrMsg = processErrMsg + ex.getMessage() + "\n";

				ex.printStackTrace();
			}
		}

	}
	// List增加一個data
	private void addData(ETL_Bean_DM_BRANCHMAPPING_LOAD_Data data) throws Exception {
		this.datas.add(data);
		this.dataCount++;

		if (dataCount == stageLimit) {
			insert_BRANCHMAPPING_LOAD_Datas();
		}
	}
	
	// 將BRANCHMAPPING資料寫入資料庫
	private void insert_BRANCHMAPPING_LOAD_Datas() throws Exception {
		if (this.datas == null || this.datas.size() == 0) {
			System.out.println("ETL_DM_BRANCHMAPPING_LOAD - insert_BRANCHMAPPING_LOAD_Datas 無寫入任何資料");
			return;
		}

		InsertAdapter insertAdapter = new InsertAdapter();
		// 呼叫寫入DB2 - SP
		insertAdapter.setSql("{call SP_INSERT_BRANCHMAPPING(?,?)}");
		// DB2 type
		insertAdapter.setCreateStructTypeName("T_BRANCHMAPPING");
		// DB2 array type
		insertAdapter.setCreateArrayTypesName("A_BRANCHMAPPING");
		insertAdapter.setTypeArrayLength(ETL_Profile.ErrorLog_Stage); // 設定上限寫入參數

		Boolean isSuccess = ETL_P_Data_Writer.insertByDefineArrayListObject2(this.datas, insertAdapter);
		int errorCount = insertAdapter.getErrorCount();

		if (isSuccess) {
			System.out.println("insert_BRANCHMAPPING_LOAD_Datas 寫入 " + this.datas.size() + "(-" + errorCount + ")筆資料!");
			this.oneFileInsertErrorCount = this.oneFileInsertErrorCount + errorCount; 
																						
		} else {
			throw new Exception("insert_BRANCHMAPPING_LOAD_Datas 發生錯誤");
		}
		// 寫入後將計數與資料List清空
		this.dataCount = 0;
		this.datas.clear();
	}
	
	public static void main(String[] args) throws Exception {
		
		String hostName = "172.18.21.208";
		String port = "22";
		String userName	= "GAMLETL";
		String password = "5325Etlpassw0rd";
		String directory = "UPLOAD/";
		Date targetDate = ETL_Tool_StringX.toUtilDate("20180510");
		String central_no = "600";
	
		String savePath = "C:\\ETL\\DM\\"+central_no;
		String fileTypeName = "BRANCHMAPPING";
		
		String batch_no = "batch1";
		
		ETL_DM_BRANCHMAPPING_LOAD obj = new  ETL_DM_BRANCHMAPPING_LOAD();
		obj.read_DM_BRANCHMAPPING_LOAD_File(hostName, port, userName, password, directory, savePath, batch_no,"600", fileTypeName, targetDate);
	}
	
	
	private void fileReaderTest() throws Exception {
		String hostName = "127.0.0.1";
		String port = "24";
		String userName	= "TEST";
		String password = "TEST";
		String directory = "UPLDAD/";
		Date date = ETL_Tool_StringX.toUtilDate("20180510");
	
		String savePath = "D:\\TESTETLDATA";
		String fileTypeName = "BRANCHMAPPING";
		
		List<File> fileList = ETL_Tool_FileReader.getTRTargetFileList(hostName, port, userName, password, directory,
				savePath, fileTypeName,date);
		


		System.out.println("共有檔案 " + fileList.size() + " 個！");
	}
	

}
