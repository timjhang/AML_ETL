package tw.com.pershing;

import java.io.File;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import ControlWS.ETL_C_Profile;
import DB.ETL_P_DMData_Writer;
import Extract.ETL_E_ACCOUNT;
import Extract.ETL_E_CALENDAR;
import Extract.ETL_E_COLLATERAL;
import Extract.ETL_E_FCX;
import Extract.ETL_E_FX_RATE;
import Extract.ETL_E_GUARANTOR;
import Extract.ETL_E_LOAN;
import Extract.ETL_E_LOAN_DETAIL;
import Extract.ETL_E_PARTY;
import Extract.ETL_E_PARTY_ADDRESS;
import Extract.ETL_E_PARTY_PARTY_REL;
import Extract.ETL_E_PARTY_PHONE;
import Extract.ETL_E_SERVICE;
import Extract.ETL_E_TRANSACTION;
import Extract.ETL_E_TRANSFER;
import Extract.ETL_E_Wrong_File;
import Migration.ETL_DM_ACCTMAPPING_LOAD;
import Migration.ETL_DM_BRANCHMAPPING_LOAD;
import Migration.ETL_DM_IDMAPPING_LOAD;
import Profile.ETL_Profile;
import Tool.ETL_Tool_Folder;
import Tool.ETL_Tool_StringX;
import tw.com.pershing.databean.ETLresponse;

@Path("/DMfunction")
public class DMfunction {
	
	// test : http://localhost:8083/AML_ETL/rest/DMfunction/WS1
	
	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public ETLresponse callDMfunction (
			@QueryParam("filePath") String filePath,
			@QueryParam("batch_no") String batch_no,
			@QueryParam("exc_central_no") String exc_central_no,
			@QueryParam("exc_record_date") String exc_record_dateStr) {
		
		System.out.println("#### DMfunction Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));

		// Web Service回傳訊息
		ETLresponse response = new ETLresponse();
		// 處理程式名稱(進行中步驟)
		String fileTypeName = "";
		// 程式代號
		String program_no = "";
		
		try {
			System.out.println(filePath);// for test
			
			/** 傳入參數基本檢核   **/
			fileTypeName = "\"傳入參數檢核\"";
			filePath = URLDecoder.decode(filePath, "UTF-8").trim();
			// 檢核傳入參數, 若有錯誤 throw相關錯誤訊息Exception
			new CheckParameter().check("c:\\", batch_no, exc_central_no, exc_record_dateStr, "001");
			Date exc_record_date = new Date();
			exc_record_date = ETL_Tool_StringX.toUtilDate(exc_record_dateStr);
			
			System.out.println("filePath:"+filePath); // for test
			System.out.println("batch_no:"+batch_no); // for test
			System.out.println("exc_central_no:"+exc_central_no); // for test
			System.out.println("exc_record_dateStr:"+exc_record_dateStr); // for test
			
			// for test
//			if (true) {
//				System.out.println("測試到這邊");
//				response.setMsg("test");
//				return response;
//			}
			

			String directory = filePath;
			String savePath = ETL_Profile.ETL_DM_SAVEPATH;
			
			List<File> list = ETL_Tool_Folder.listFilesForFolder(new File(savePath), ETL_Tool_Folder.Type.ALLFileKeepFolder);
			ETL_Tool_Folder.dropFileByList(list);

			ETL_P_DMData_Writer.truncateTable("ERROR_LOG");
			ETL_P_DMData_Writer.truncateTable("ACCTMAPPING");
			ETL_P_DMData_Writer.truncateTable("BRANCHMAPPING");
			ETL_P_DMData_Writer.truncateTable("IDMAPPING");
			
			fileTypeName = "ACCTMAPPING";
			new ETL_DM_ACCTMAPPING_LOAD().read_DM_ACCTMAPPING_LOAD_File(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, directory, savePath, batch_no, exc_central_no,fileTypeName, exc_record_date);
			
			fileTypeName = "BRANCHMAPPING";
			new ETL_DM_BRANCHMAPPING_LOAD().read_DM_BRANCHMAPPING_LOAD_File(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, directory, savePath, batch_no,exc_central_no ,fileTypeName, exc_record_date);
			
			fileTypeName = "IDMAPPING";
			new ETL_DM_IDMAPPING_LOAD().read_DM_IDMAPPING_LOAD_File(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, directory, savePath, batch_no,exc_central_no ,fileTypeName, exc_record_date);
			
			// 執行成功
			response.setMsg("SUCCESS");
			
		} catch (Exception ex) {
			ex.printStackTrace();
			response.setMsg("Exception");
			response.setError("執行" + fileTypeName + "發生錯誤:" + ex.getMessage());
		}
		
		System.out.println("#### DMfunction End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		return response;
	}
	
}
