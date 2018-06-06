package tw.com.pershing;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

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
import Extract.Extract;
import Profile.ETL_Profile;
import Tool.ETL_Tool_StringX;
import tw.com.pershing.databean.ETLresponse;

@Path("/Efunction")
public class Efunction {
	
	// test : http://localhost:8083/AML_ETL/rest/Efunction/WS1
	
	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public ETLresponse callEfunction (
			@QueryParam("filePath") String filePath,
//			@QueryParam("fileTypeName") String fileTypeName,
			@QueryParam("batch_no") String batch_no,
			@QueryParam("exc_central_no") String exc_central_no,
			@QueryParam("exc_record_date") String exc_record_dateStr,
			@QueryParam("upload_no") String upload_no/*,
			@QueryParam("program_no") String program_no */) {
		
		System.out.println("#### Efunction Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));

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
			new CheckParameter().check(filePath, batch_no, exc_central_no, exc_record_dateStr, upload_no);
			Date exc_record_date = new Date();
			exc_record_date = ETL_Tool_StringX.toUtilDate(exc_record_dateStr);
			
			System.out.println(filePath); // for test
			System.out.println(batch_no); // for test
			System.out.println(exc_central_no); // for test
			System.out.println(exc_record_dateStr); // for test
			System.out.println(upload_no); // for test
			
			// for test
//			if (true) {
//				System.out.println("測試到這邊");
//				response.setMsg("test");
//				return response;
//			}
			
			
//			// 執行15支E系列程式
//			fileTypeName = "PARTY";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_PARTY().read_Party_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "ACCOUNT";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_ACCOUNT().read_Account_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "CALENDAR";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_CALENDAR().read_CALENDAR_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "COLLATERAL";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_COLLATERAL().read_Collateral_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "FCX";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_FCX().read_FCX_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "FX_RATE";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_FX_RATE().read_Fx_Rate_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "GUARANTOR";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_GUARANTOR().read_Guarantor_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "LOAN_DETAIL";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_LOAN_DETAIL().read_Loan_Detail_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "LOAN";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_LOAN().read_Loan_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "PARTY_ADDRESS";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_PARTY_ADDRESS().read_Party_Address_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "PARTY_PARTY_REL";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_PARTY_PARTY_REL().read_Party_Party_Rel_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "PARTY_PHONE";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_PARTY_PHONE().read_Party_Phone_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "SERVICE";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_SERVICE().read_Service_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "TRANSACTION";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_TRANSACTION().read_Transaction_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "TRANSFER";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_TRANSFER().read_Transfer_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
//			
//			fileTypeName = "Wrong_File";
//			program_no = "ETL_E_" + fileTypeName;
//			new ETL_E_Wrong_File().read_Error_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no);
			
			List<Extract> extracts = new ArrayList<Extract>();
			
			fileTypeName = "TRANSACTION";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_TRANSACTION(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "PARTY";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_PARTY(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "ACCOUNT";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_ACCOUNT(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "CALENDAR";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_CALENDAR(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "COLLATERAL";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_COLLATERAL(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "FCX";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_FCX(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "FX_RATE";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_FX_RATE(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "GUARANTOR";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_GUARANTOR(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "LOAN_DETAIL";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_LOAN_DETAIL(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "LOAN";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_LOAN(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "PARTY_ADDRESS";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_PARTY_ADDRESS(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "PARTY_PARTY_REL";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_PARTY_PARTY_REL(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "PARTY_PHONE";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_PARTY_PHONE(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "SERVICE";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_SERVICE(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "TRANSFER";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_TRANSFER(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "Wrong_File";
			program_no = "ETL_E_" + fileTypeName;
			extracts.add(new ETL_E_Wrong_File(filePath, fileTypeName, batch_no, exc_central_no, exc_record_date, upload_no, program_no));
			
			fileTypeName = "E多線程";
			ExecutorService executor = Executors.newFixedThreadPool(3);
			
			for (Extract extract : extracts) {
				executor.execute(extract);
			}
			
			executor.shutdown();

			while (!executor.isTerminated()) {

			}
			
			System.out.println("線程池已經關閉");
			
			// 執行成功
			response.setMsg("SUCCESS");
			
			// for test
			List<String> logs = new ArrayList<String>();
			logs.add("Tim");
			logs.add("Ian");
			logs.add("Kevin");
			response.setLogs(logs);
		
		} catch (Exception ex) {
			ex.printStackTrace();
			response.setMsg("Exception");
			response.setError("執行" + fileTypeName + "發生錯誤:" + ex.getMessage());
		}
		
		System.out.println("#### Efunction End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		return response;
	}
	
}
