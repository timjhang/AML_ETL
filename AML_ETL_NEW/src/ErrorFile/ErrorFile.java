package ErrorFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import Bean.ETL_Bean_ERROR_LOG_DOWNLOAD;
import Bean.ETL_Bean_ERROR_LOG_DOWNLOAD_FILE;
import ControlWS.ETL_C_Profile;
import FTP.ETL_SFTP;
import Tool.ETL_Tool_ZIP;

public class ErrorFile {

	//上傳直接寫在控制那就好不須另外開FUNCTION
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		// 下載error 且壓縮
		File zip = ErrorFile.getErrorFileZip("D:\\20180709","D:\\20180709\\temp.zip", "D:\\20180709\\temp.txt", "password");
		
		String remoteFilePath = "\\test1.zip";
		if (zip != null) {
			// 上傳 sftp file
			ETL_SFTP.upload(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username,ETL_C_Profile.sftp_password, zip.getAbsolutePath(), remoteFilePath);
		}
	}
	
	/**
	 * 
	 * @param directory
	 *            存放error.txt 的目錄
	 * @param zipFilePath
	 *            要存放zip的絕對位置
	 * @param zipPW
	 *            密碼
	 * @return
	 * @throws IOException 
	 */
	// 若有錯誤檔案則回傳zip壓縮檔, 若無錯誤檔案則回傳空txt檔案
	public static File getErrorFileZip(String directory, String zipFilePath, String txtFilePath, String zipPW) {
		File file = new File(directory);
		if (file.exists()) {
			file.delete();
		}

		file.mkdirs();

		try {
			if (error_log_download(directory)) {
				File[] filesArr = file.listFiles();
				ArrayList<File> files = new ArrayList<File>();
	
				for (File f : filesArr) {
					files.add(f);
				}
				
				if (files != null && files.size() != 0) {
					return ETL_Tool_ZIP.toZip(files, zipFilePath, zipPW);
				} else {
					File txtFile = new File(txtFilePath);
					if (txtFile.exists()) {
						txtFile.delete();
					}
					txtFile.createNewFile();
					
					return txtFile;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return null;
	}

	// 寫入txt D:\ETL\DB600\UPLOAD\20180705\001
	public static boolean error_log_download(String directory) {
		List<ETL_Bean_ERROR_LOG_DOWNLOAD_FILE> files;

		try {
			files = ErrorFileDAO.getErrorLogDownloadFile();

			for (ETL_Bean_ERROR_LOG_DOWNLOAD_FILE file : files) {
				System.out.println(directory + "\\" + file.getFile_name());

				PrintWriter writer = new PrintWriter(directory + "\\" + file.getFile_name(), "UTF-8");
				List<ETL_Bean_ERROR_LOG_DOWNLOAD> rows = file.getBody();

				for (ETL_Bean_ERROR_LOG_DOWNLOAD row : rows) {
					writer.println(row.getSrc_data() + row.getMark_1()
						+ row.getRow_count() + row.getMark_2()
						+ row.getField_name() + row.getMark_3()
						+ row.getError_description());
				}
				writer.close();
			}

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

}
