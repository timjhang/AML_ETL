package tw.com.pershing;

import java.io.File;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import ControlWS.ETL_C_Profile;
import ErrorFile.ErrorFile;
import ErrorFile.ErrorFileDAO;
import FTP.ETL_SFTP;
import Tool.ETL_Tool_FileName_Encrypt;
import tw.com.pershing.databean.ETLresponse;

@Path("/UploadErrorFile")
public class UploadErrorFile {

	// test : http://localhost:8083/AML_ETL/rest/UploadErrorFile/WS1?action=clear
	
	// 清除 ERROR_LOG_DOWNLOAD, 同時重整table
	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public static ETLresponse clear_ERROR_LOG_DOWNLOAD(@QueryParam("action") String action) {
		
		ETLresponse response = new ETLresponse();
		
		if ("clear".equals(action)) {
			
			try {
				
				if (ErrorFileDAO.clear_table_ERROR_LOG_DOWNLOAD()) {
					response.setMsg("SUCCESS");
				} else {
					response.setMsg("FAILURE");
					response.setError("執行clear_table_ERROR_LOG_DOWNLOAD 失敗");
				}
		  		
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
		} else {
			response.setMsg("FAILURE");
			response.setError("呼叫字串非clear");
		}
		
		return response;
	}
	
	// test : http://localhost:8083/AML_ETL/rest/UploadErrorFile/WS2?recordDateStr=20181026&centralNo=600&uploadNo=001
	
	// ERROR_LOG_DOWNLOAD資料, 上傳SFTP
	@GET
	@Path("/WS2")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public static ETLresponse upload_ERROR_LOG_DOWNLOAD(
			@QueryParam("recordDateStr") String recordDateStr, @QueryParam("centralNo") String centralNo,
			@QueryParam("uploadNo") String uploadNo) {
		
		ETLresponse response = new ETLresponse();
		
		try {
			System.out.println("打包錯誤回覆檔  資料日期：" + recordDateStr + " , 中心:" + centralNo);
			
			boolean executeResult = false;
			
			String ouputErrorFilePath = "D:\\ETL\\DB" + centralNo + "\\UPLOAD\\" + recordDateStr + "\\" + uploadNo;
			File errorFileFolder = new File(ouputErrorFilePath);
			// 產生路徑資料夾
			if (!errorFileFolder.exists() || !errorFileFolder.isDirectory()) {
				errorFileFolder.mkdirs();
			}
			
			// 錯誤檔案產出Error_File
			ErrorFile.error_log_download(errorFileFolder.getAbsolutePath());
			
			File zipFolder = new File("D:\\ETL\\DB" + centralNo + "\\UPLOAD\\" + recordDateStr + "\\" + uploadNo + "zip");
			// 產生路徑資料夾
			if (!zipFolder.exists() || !zipFolder.isDirectory()) {
				zipFolder.mkdirs();
			}
			
			String zipName = recordDateStr + ".zip";
			String txtName = recordDateStr + ".txt";
			String password = ETL_Tool_FileName_Encrypt.encode(zipName);
			
			// 下載error 且壓縮, 若無錯誤檔案則產生空txt檔
			File zipFile = ErrorFile.getErrorFileZip(errorFileFolder.getAbsolutePath(), 
					zipFolder + "\\" + zipName, zipFolder + "\\" + txtName, password);
			
//			String ErrorFolderPath = "//" + centralNo + "//DOWNLOAD//ERROR";
//			boolean isErrorFolderExists = true;
//			if (!ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username,ETL_C_Profile.sftp_password, 
//					ErrorFolderPath)) {
//				System.out.println("須產生" + ErrorFolderPath);
//				isErrorFolderExists = ETL_SFTP.createFolder(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username,ETL_C_Profile.sftp_password, 
//						ErrorFolderPath);
//				if (isErrorFolderExists) {
//					System.out.println(ErrorFolderPath + "產生成功");
//				} else {
//					System.out.println(ErrorFolderPath + "產生失敗");
//				}
//			}
			
//			if (isErrorFolderExists) {
			String remoteErrorFileFolder = "//" + centralNo + "//DOWNLOAD//ERROR//" + recordDateStr.substring(0, 6);
			System.out.println("remoteErrorFileFolder = " + remoteErrorFileFolder);
			boolean isErrorFileFolderExists = true;
			if (!ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username,ETL_C_Profile.sftp_password, 
					remoteErrorFileFolder)) {
				System.out.println("須產生" + remoteErrorFileFolder);
				isErrorFileFolderExists = ETL_SFTP.createFolder(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username,ETL_C_Profile.sftp_password, 
						remoteErrorFileFolder);
				if (isErrorFileFolderExists) {
					System.out.println(remoteErrorFileFolder + "產生成功");
				} else {
					System.out.println(remoteErrorFileFolder + "產生失敗");
				}
			}
			
			if (isErrorFileFolderExists) {
				System.out.println("zipFile AbsolutePath = " + zipFile.getAbsolutePath());
				// 上傳 sftp file
				if (ETL_SFTP.upload(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username,ETL_C_Profile.sftp_password,
						zipFile.getAbsolutePath(), remoteErrorFileFolder + "//" + zipFile.getName())) {
					executeResult = true;
				}
			}
//			}
			
			if (executeResult) {
				response.setMsg("SUCCESS");
			} else {
				response.setMsg("FAILURE");
				response.setError("執行upload_ERROR_LOG_DOWNLOAD 失敗");
			}
		
		} catch (Exception ex) {
			ex.printStackTrace();
			response.setMsg("FAILURE");
			response.setError(ex.getMessage());
		}
		
		return response;
	}
	
	public static void main(String[] argvs) {
//		upload_ERROR_LOG_DOWNLOAD("20181126", "018", "001");
	}
}
