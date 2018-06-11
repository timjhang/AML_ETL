package ControlWS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import DB.ETL_P_Log;
import FTP.ETL_SFTP;
import Tool.ETL_Tool_FileName_Encrypt;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_ZIP;

public class ETL_C_GET_UPLOAD_FILE {
	
	// 下載ETL 壓縮檔
	public static boolean download_SFTP_Files(String central_no, String[] downloadFileInfo) {
		
		try {
			
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_Files Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			
			// 待處理共用中心代號
			String central_No = central_no;
			
			// 搜尋MASTER檔, 取得 List<資料日期|上傳批號 |zip檔名>
//			String[] dataInfo = new String[2];
			List<String> zipFiles = new ArrayList<String>();
			try {
				zipFiles = parseMasterTxt(central_No);
			} catch (Exception ex) {
				System.out.println("解析" + central_No + "Master檔出現問題!");
				ex.printStackTrace();
			}
			
			// 若存在則進行開路徑 & 下載 & 解壓縮作業
			if (zipFiles != null) {
				for (int file_index = 0; file_index < zipFiles.size(); file_index++) {
					String[] dataInfo = zipFiles.get(file_index).split("\\|");
					
					// 正常情況只會有一筆, 這邊只取第一筆
					if (file_index == 0) {
						downloadFileInfo[0] = zipFiles.get(file_index);
					}
					
					// 檢核資料日期 + 上傳批號, 是否曾經有跑過  ????
					
					
					File downloadDir = new File(ETL_C_Profile.ETL_Download_localPath + central_No + "/" + dataInfo[0] + "/" + dataInfo[1]);
					
					if (!downloadDir.exists()) {
						downloadDir.mkdirs();
					}
					// for test
//						else {
//							System.out.println(file.getAbsolutePath() + " 已經存在, 請確認是否重複執行。");
//							continue;
//						}
					
					// 下載ZIP檔
					if (downloadZipFile(central_No, dataInfo[2])) {
						System.out.println("下載檔案:" + dataInfo[2] + " 成功!");
					} else {
						System.out.println("下載檔案:" + dataInfo[2] + " 發生錯誤!");
						break;
					}
					
					// 解壓縮檔案到新批號目錄底下
					String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD/" + dataInfo[2];
					String localExtractDir = ETL_C_Profile.ETL_Download_localPath + central_No + "/" + dataInfo[0] + "/" + dataInfo[1] + "/";
					String password = ETL_Tool_FileName_Encrypt.encode(dataInfo[2]);
					if (ETL_Tool_ZIP.extractZipFiles(localDownloadFilePath, localExtractDir, password)) {
						System.out.println("解壓縮localDownloadFilePath:" + localDownloadFilePath );
						System.out.println("解壓縮localExtractDir:" + localExtractDir );
						System.out.println("解壓縮password:" + password );
						
						System.out.println("解壓縮檔案:" + dataInfo[2] + " 成功！");
						
						// 紀錄解壓縮成功
						// ????
					} else {
						System.out.println("解壓縮localDownloadFilePath:" + localDownloadFilePath );
						System.out.println("解壓縮localExtractDir:" + localExtractDir );
						System.out.println("解壓縮password:" + password );
						
						System.out.println("解壓縮檔案:" + dataInfo[2] + " 失敗！");
						// 紀錄解壓縮失敗
						// ????
					}
					
				}
				
				// 下載檔案解壓縮成功後, 刪除目錄上Master檔
				removeMasterTxt(central_No);
				
			} else {
				// 若無收到Master檔, 給出訊息
				System.out.println(central_No + " 查無Master檔, 不進行下載。");
			}
				
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_Files End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_Files 發生錯誤!! " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			return false;
		}
		
	}
	
	// 下載Rerun壓縮檔
	public static boolean download_SFTP_RerunFiles(String central_no, String[] downloadFileInfo, String rerunRecordDateStr) {
		
		try {
			
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_RerunFiles Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			
			// 待處理共用中心代號
			String central_No = central_no;
			
			// 搜尋MASTER檔, 取得 List<資料日期|上傳批號 |zip檔名>
//			String[] dataInfo = new String[2];
			List<String> zipFiles = new ArrayList<String>();
			try {
				zipFiles = parseRerunTxt(central_No, rerunRecordDateStr);
			} catch (Exception ex) {
				System.out.println("解析" + central_No + "Rerun檔出現問題!");
				ex.printStackTrace();
			}
			
			// 若存在則進行開路徑 & 下載 & 解壓縮作業
			if (zipFiles != null) {
				for (int file_index = 0; file_index < zipFiles.size(); file_index++) {
					String[] dataInfo = zipFiles.get(file_index).split("\\|");
					
					// 正常情況只會有一筆, 這邊只取第一筆
					if (file_index == 0) {
						downloadFileInfo[0] = zipFiles.get(file_index);
					}
					
					// 檢核資料日期 + 上傳批號, 是否曾經有跑過  ????
					
					
					File downloadDir = new File(ETL_C_Profile.ETL_Download_localPath + central_No + "/" + dataInfo[0] + "/" + dataInfo[1]);
					
					if (!downloadDir.exists()) {
						downloadDir.mkdirs();
					}
					// for test
//						else {
//							System.out.println(file.getAbsolutePath() + " 已經存在, 請確認是否重複執行。");
//							continue;
//						}
					
					// 下載ZIP檔
					if (downloadZipFile(central_No, dataInfo[2])) {
						System.out.println("下載檔案:" + dataInfo[2] + " 成功!");
					} else {
						System.out.println("下載檔案:" + dataInfo[2] + " 發生錯誤!");
						break;
					}
					
					// 解壓縮檔案到新批號目錄底下
					String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD/" + dataInfo[2];
					String localExtractDir = ETL_C_Profile.ETL_Download_localPath + central_No + "/" + dataInfo[0] + "/" + dataInfo[1] + "/";
					String password = ETL_Tool_FileName_Encrypt.encode(dataInfo[2]);
					// 先清除資料底下舊有檔案
					deleteDirFiles(ETL_C_Profile.ETL_Download_localPath + central_No + "/" + dataInfo[0] + "/" + dataInfo[1]);
					if (ETL_Tool_ZIP.extractZipFiles(localDownloadFilePath, localExtractDir, password)) {
						System.out.println("解壓縮localDownloadFilePath:" + localDownloadFilePath );
						System.out.println("解壓縮localExtractDir:" + localExtractDir );
						System.out.println("解壓縮password:" + password );
						
						System.out.println("解壓縮檔案:" + dataInfo[2] + " 成功！");
						
						// 紀錄解壓縮成功
						// ????
					} else {
						System.out.println("解壓縮localDownloadFilePath:" + localDownloadFilePath );
						System.out.println("解壓縮localExtractDir:" + localExtractDir );
						System.out.println("解壓縮password:" + password );
						
						System.out.println("解壓縮檔案:" + dataInfo[2] + " 失敗！");
						// 紀錄解壓縮失敗
						// ????
					}
					
				}
				
				// 下載檔案解壓縮成功後, 刪除目錄上Rerun檔
				removeRerunTxt(central_No, new SimpleDateFormat("yyyyMMdd").parse(rerunRecordDateStr));
				
			} else {
				// 若無收到Master檔, 給出訊息
				System.out.println(central_No + " 查無Rerun檔, 不進行下載。");
			}
				
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_RerunFiles End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_RerunFiles 發生錯誤!! " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			return false;
		}
		
	}
	
	// 下載Migration 壓縮檔
	public static boolean download_SFTP_MigrationFiles(String central_no, String[] downloadFileInfo) {
		
		try {
			
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_MigrationFiles Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			
			// 待處理共用中心代號
			String central_No = central_no;
			
			// 搜尋MASTER檔, 取得 List<資料日期|上傳批號 |zip檔名>
//				String[] dataInfo = new String[2];
			List<String> zipFiles = new ArrayList<String>();
			try {
				zipFiles = parseMigrationMasterTxt(central_No);
			} catch (Exception ex) {
				System.out.println("解析" + central_No + "Master檔出現問題!");
				ex.printStackTrace();
			}
			
			// 若存在則進行開路徑 & 下載 & 解壓縮作業
			if (zipFiles != null) {
				for (int file_index = 0; file_index < zipFiles.size(); file_index++) {
					String[] dataInfo = zipFiles.get(file_index).split("\\|");
					
					// 正常情況只會有一筆, 這邊只取第一筆
					if (file_index == 0) {
						downloadFileInfo[0] = zipFiles.get(file_index);
					}
					
					File downloadDir = new File(ETL_C_Profile.ETL_Download_localPath + central_No + "/Migration/" + dataInfo[0] + "/" + dataInfo[1]);
					
					if (!downloadDir.exists()) {
						downloadDir.mkdirs();
					}
					// for test
//							else {
//								System.out.println(file.getAbsolutePath() + " 已經存在, 請確認是否重複執行。");
//								continue;
//							}
					
					// 下載ZIP檔
					if (downloadMigrationZipFile(central_No, dataInfo[2])) {
						System.out.println("下載檔案:" + dataInfo[2] + " 成功!");
					} else {
						System.out.println("下載檔案:" + dataInfo[2] + " 發生錯誤!");
						break;
					}
					
					// 解壓縮檔案到新批號目錄底下
					String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD/" + dataInfo[2];
					String localExtractDir = ETL_C_Profile.ETL_Download_localPath + central_No + "/Migration/" + dataInfo[0] + "/" + dataInfo[1] + "/";
					String password = ETL_Tool_FileName_Encrypt.encode(dataInfo[2]);
					// 先清除資料底下舊有檔案
					deleteDirFiles(ETL_C_Profile.ETL_Download_localPath + central_No + "/Migration/" + dataInfo[0] + "/" + dataInfo[1]);
					if (ETL_Tool_ZIP.extractZipFiles(localDownloadFilePath, localExtractDir, password)) {
						System.out.println("解壓縮localDownloadFilePath:" + localDownloadFilePath );
						System.out.println("解壓縮localExtractDir:" + localExtractDir );
						System.out.println("解壓縮password:" + password );
						
						System.out.println("解壓縮檔案:" + dataInfo[2] + " 成功！");
						
						// 紀錄解壓縮成功
						// ????
						
						//檔案進行更名
						if (renameMigrationFiles(localExtractDir)) {
							System.out.println(localExtractDir + "  內檔案去  \"TR_\"  結束!");
						} else {
							System.out.println(localExtractDir + "  內檔案去  \"TR_\"  失敗!");
						}
						
					} else {
						System.out.println("解壓縮localDownloadFilePath:" + localDownloadFilePath );
						System.out.println("解壓縮localExtractDir:" + localExtractDir );
						System.out.println("解壓縮password:" + password );
						
						System.out.println("解壓縮檔案:" + dataInfo[2] + " 失敗！");
						// 紀錄解壓縮失敗
						// ????
					}
					
				}
				
				// 下載檔案解壓縮成功後, 刪除目錄上Master檔
				removeMigrationMasterTxt(central_No);
				
			} else {
				// 若無收到Master檔, 給出訊息
				System.out.println(central_No + " 查無TR Master檔, 不進行下載。");
			}
				
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_MigrationFiles End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("#### ETL_C_GET_UPLOAD_FILE - download_SFTP_MigrationFiles 發生錯誤!! " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			return false;
		}
		
	}
	
	// 確認是否有對應Master檔
	private static List<String> parseMasterTxt(String central_No) throws Exception {
		
		// 結果字串
		List<String> resultList = new ArrayList<String>();
		
		String masterFileName = central_No + "MASTER.txt";
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		boolean hasMaster = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile);
		
		if (!hasMaster) {
			System.out.println("找不到" + remoteMasterFile + "檔案!");
			return null;
		}
		
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD";
		File localDownloadFileDir = new File(localDownloadFilePath);
		if (!localDownloadFileDir.exists()) {
			localDownloadFileDir.mkdir();
		}
		
		String localMasterFile = localDownloadFilePath 
				+ "/" + central_No + "MASTER_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";
		
//		System.out.println(localMasterFile); // for test
//		System.out.println(remoteMasterFile); // for test
		
		// download Master檔
		if (ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localMasterFile, remoteMasterFile)) {
			System.out.println("Download: " + remoteMasterFile + " 成功!");
		} else {
			System.out.println("Download: " + remoteMasterFile + " 失敗!");
			return null;
			// throw exception
		}
			
		// 讀取master檔內明細資料, 回傳zip檔list
		File parseFile = new File(localMasterFile);
		FileInputStream fis = new FileInputStream(parseFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
		
		String masterLineStr = "";
		String resultStr = "";
		while (br.ready()) {
			masterLineStr = br.readLine();
			System.out.println(masterLineStr); // for test
			
			resultStr = checkMasterLineString(central_No, masterLineStr);
			if (resultStr == null) {
				throw new Exception("解析" + parseFile.getName() + "解析出現問題!");
			}
			
			resultList.add(resultStr);
		}
		
		br.close();
		fis.close();
		
		return resultList;
	}
	
	// 確認是否有對應Rerun檔
	private static List<String> parseRerunTxt(String central_No, String rerunRecordDateStr) throws Exception {
		
		// 結果字串
		List<String> resultList = new ArrayList<String>();
		
		String rerunFileName = central_No + "_RERUN_" + rerunRecordDateStr + ".txt";
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String remoteRerunFile = remoteFilePath + rerunFileName;
		boolean hasRerun = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteRerunFile);
		
		if (!hasRerun) {
			System.out.println("找不到" + remoteRerunFile + "檔案!");
			return null;
		}
		
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD";
		File localDownloadFileDir = new File(localDownloadFilePath);
		if (!localDownloadFileDir.exists()) {
			localDownloadFileDir.mkdir();
		}
		
		String localRerunFile = localDownloadFilePath 
				+ "/" + central_No +"_RERUN_" + rerunRecordDateStr + "_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";
		
//			System.out.println(localMasterFile); // for test
//			System.out.println(remoteMasterFile); // for test
		
		// download Rerun檔
		if (ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localRerunFile, remoteRerunFile)) {
			System.out.println("Download: " + remoteRerunFile + " 成功!");
		} else {
			System.out.println("Download: " + remoteRerunFile + " 失敗!");
			return null;
			// throw exception
		}
			
		// 讀取master檔內明細資料, 回傳zip檔list
		File parseFile = new File(localRerunFile);
		FileInputStream fis = new FileInputStream(parseFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
		
		String rerunLineStr = "";
		String resultStr = "";
		while (br.ready()) {
			rerunLineStr = br.readLine();
			System.out.println(rerunLineStr); // for test
			
			resultStr = checkMasterLineString(central_No, rerunLineStr);
			if (resultStr == null) {
				throw new Exception("解析" + parseFile.getName() + "解析出現問題!");
			}
			
			resultList.add(resultStr);
		}
		
		br.close();
		fis.close();
		
		return resultList;
	}
	
	// 確認是否有對應Migration檔
	private static List<String> parseMigrationMasterTxt(String central_No) throws Exception {
		
		// 結果字串
		List<String> resultList = new ArrayList<String>();
		
		String masterFileName =  "TR_" + central_No + "MASTER.txt";
		String remoteFilePath = "/" + central_No + "/MIGRATION/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		boolean hasMaster = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile);
		
		if (!hasMaster) {
			System.out.println("找不到" + remoteMasterFile + "檔案!");
			return null;
		}
		
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD";
		File localDownloadFileDir = new File(localDownloadFilePath);
		if (!localDownloadFileDir.exists()) {
			localDownloadFileDir.mkdir();
		}
		
		String localMasterFile = localDownloadFilePath 
				+ "/TR_" + central_No + "MASTER_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";
		
//			System.out.println(localMasterFile); // for test
//			System.out.println(remoteMasterFile); // for test
		
		// download Master檔
		if (ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localMasterFile, remoteMasterFile)) {
			System.out.println("Download: " + remoteMasterFile + " 成功!");
		} else {
			System.out.println("Download: " + remoteMasterFile + " 失敗!");
			return null;
			// throw exception
		}
			
		// 讀取master檔內明細資料, 回傳zip檔list
		File parseFile = new File(localMasterFile);
		FileInputStream fis = new FileInputStream(parseFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
		
		String migrationMasterLineStr = "";
		String resultStr = "";
		while (br.ready()) {
			migrationMasterLineStr = br.readLine();
			System.out.println(migrationMasterLineStr); // for test
			
			resultStr = checkMigrationMasterLineString(central_No, migrationMasterLineStr);
			if (resultStr == null) {
				throw new Exception("解析" + parseFile.getName() + "解析出現問題!");
			}
			
			resultList.add(resultStr);
		}
		
		br.close();
		fis.close();
		
		return resultList;
	}
	
	// 解析檢核Master File當中String
	private static String checkMasterLineString(String central_No, String input) {
		try {
			String[] strAry =  input.split("\\,");
			if (strAry.length != 2) {
				System.out.println("無法以,分隔"); // for test
				return null;
			}
			
			// 檢核record_Date + upload_No
			if (strAry[0].length() != 11) {
				System.out.println("格式不正確, 前字長度不足。"); // for test
				return null;
			}
			
			// 資料日期
			String record_Date = strAry[0].substring(0, 8);
			// 上傳批號
			String upload_No = strAry[0].substring(8, 11);
			
			// 檢核日期格式
			if (!ETL_Tool_FormatCheck.checkDate(record_Date)) {
				return null;
			}
			
			// 檢核zip檔名
			String zipFileName = "AML_" + central_No + "_" + strAry[0] + ".zip";
			if (!zipFileName.equals(strAry[1])) {
				System.out.println("檔名檢核不通過:" + zipFileName + " - " + strAry[1]); // for test
				return null;
			}
			
			// 回傳  "(資料日期)|(上傳批號)|(zip檔名)"
			return record_Date + "|" + upload_No + "|" + zipFileName;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("發生錯誤"); // for test
			return null;
		}
	}
	
	// 解析檢核Migration Master File當中String
	private static String checkMigrationMasterLineString(String central_No, String input) {
		try {
			String[] strAry =  input.split("\\,");
			if (strAry.length != 2) {
				System.out.println("無法以,分隔"); // for test
				return null;
			}
			
			// 檢核record_Date + upload_No
			if (strAry[0].length() != 11) {
				System.out.println("格式不正確, 前字長度不足。"); // for test
				return null;
			}
			
			// 資料日期
			String record_Date = strAry[0].substring(0, 8);
			// 上傳批號
			String upload_No = strAry[0].substring(8, 11);
			
			// 檢核日期格式
			if (!ETL_Tool_FormatCheck.checkDate(record_Date)) {
				return null;
			}
			
			// 檢核zip檔名
			String zipFileName = "AML_TR_" + central_No + "_" + strAry[0] + ".zip";
			if (!zipFileName.equals(strAry[1])) {
				System.out.println("檔名檢核不通過:" + zipFileName + " - " + strAry[1]); // for test
				return null;
			}
			
			// 回傳  "(資料日期)|(上傳批號)|(zip檔名)"
			return record_Date + "|" + upload_No + "|" + zipFileName;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("發生錯誤"); // for test
			return null;
		}
	}
	
	// 下載zip檔案
	private static boolean downloadZipFile(String central_No, String zipFileName) {
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD/";
		String remoteFile = remoteFilePath + zipFileName;
		String localDownloadFile = localDownloadFilePath + zipFileName;
		
		// TODO FOR TEST
		System.out.println("downloadFileInfo.... " );
		System.out.println("central_No: "+central_No );
		System.out.println("zipFileName: "+zipFileName );
		System.out.println("remoteFilePath: "+remoteFilePath );
		System.out.println("localDownloadFilePath: "+localDownloadFilePath );
		System.out.println("remoteFile: "+remoteFile );
		System.out.println("localDownloadFile: "+localDownloadFile );

		
		boolean downLoadOK = ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localDownloadFile, remoteFile);
		
		return downLoadOK;
	}
	
	// 下載zip檔案
	private static boolean downloadMigrationZipFile(String central_No, String zipFileName) {
		String remoteFilePath = "/" + central_No + "/MIGRATION/";
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD/";
		String remoteFile = remoteFilePath + zipFileName;
		String localDownloadFile = localDownloadFilePath + zipFileName;
		
		// TODO FOR TEST
		System.out.println("downloadFileInfo.... " );
		System.out.println("central_No: "+central_No );
		System.out.println("zipFileName: "+zipFileName );
		System.out.println("remoteFilePath: "+remoteFilePath );
		System.out.println("localDownloadFilePath: "+localDownloadFilePath );
		System.out.println("remoteFile: "+remoteFile );
		System.out.println("localDownloadFile: "+localDownloadFile );

		
		boolean downLoadOK = ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localDownloadFile, remoteFile);
		
		return downLoadOK;
	}
	
	// 刪除SFTP上Master.txt檔
	private static boolean removeMasterTxt(String central_no) {
		
		String masterFileName = central_no + "MASTER.txt";
		String remoteFilePath = "/" + central_no + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		
		if (ETL_SFTP.delete(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile)) {
			
			System.out.println("刪除 " + remoteMasterFile + " 成功！");
			return true;
		} else {
			
			System.out.println("刪除 " + remoteMasterFile + " 失敗！");
			return false;
		}
		
	}
	
	// 刪除SFTP上XXX_RERUN_yyyyMMdd.txt檔
	private static boolean removeRerunTxt(String central_no, Date record_date) {
		
		String rerunFileName = central_no + "_RERUN_" + new SimpleDateFormat("yyyyMMdd").format(record_date) + ".txt";
		String remoteFilePath = "/" + central_no + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + rerunFileName;
		
		if (ETL_SFTP.delete(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile)) {
			
			System.out.println("刪除 " + remoteMasterFile + " 成功！");
			ETL_P_Log.write_Runtime_Log("removeMasterTxt", "刪除 " + remoteMasterFile + " 成功！");
			return true;
		} else {
			
			System.out.println("刪除 " + remoteMasterFile + " 失敗！");
			ETL_P_Log.write_Runtime_Log("removeMasterTxt", "刪除 " + remoteMasterFile + " 失敗！");
			return false;
		}
		
	}
	
	// 刪除SFTP上Master.txt檔
	private static boolean removeMigrationMasterTxt(String central_no) {
		
		String masterFileName = "TR_" + central_no + "MASTER.txt";
		String remoteFilePath = "/" + central_no + "/MIGRATION/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		
		if (ETL_SFTP.delete(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile)) {
			
			System.out.println("刪除 " + remoteMasterFile + " 成功！");
			return true;
		} else {
			
			System.out.println("刪除 " + remoteMasterFile + " 失敗！");
			return false;
		}
		
	}
	
	// Migration Files更新
	private static boolean renameMigrationFiles(String migrationFilesExtractDir) {
		
		File file = new File(migrationFilesExtractDir);
		
		if (file == null || !file.isDirectory()) {
			return false;
		}
		
		boolean isSuccess = true;
		
		try {
		
			File[] files = file.listFiles();
			for (int i = 0; i < files.length; i++) {
				System.out.println(files[i].getName());
				File newFile = new File(migrationFilesExtractDir + "/" + files[i].getName().substring(3));
				if (files[i].renameTo(newFile)) {
					System.out.println("rename to : " + newFile.getName() + "  成功!!");
				} else {
					System.out.println("rename to : " + newFile.getName() + "  失敗!");
					isSuccess = false;
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			isSuccess = false;
		}
		
		return isSuccess;
	}
	
	// 路徑下檔案清除
	private static void deleteDirFiles(String path) {
		
		File file = new File(path);
		if (file.isDirectory()) {
			File[] fileArray = file.listFiles();
			for (int i = 0; i < fileArray.length; i++) {
				if (fileArray[i].isFile()) {
					fileArray[i].delete();
				}
			}
		} else {
			System.out.println(path + " 非路徑！");
		}
	}

	public static void main(String[] args) {
		
//		String[] downloadFileInfo = new String[1];
//		download_SFTP_Files("600", downloadFileInfo);
//		System.out.println("downloadFileInfo = " + downloadFileInfo[0]);
		
		if (renameMigrationFiles("C:/Users/10404003/Desktop/農金/2018/180609/test")) {
			System.out.println("rename success!");
		} else {
			System.out.println("rename failure!");
		}
	}

}
