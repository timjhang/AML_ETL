package tw.com.pershing;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import Bean.ETL_Bean_LogData;
import Tool.ETL_Tool_StringX;
import Transform.ETL_T_ACCOUNT_PROPERTY;
import Transform.ETL_T_ACCOUNT;
import Transform.ETL_T_BALANCE;
import Transform.ETL_T_CALENDAR_LOAD;
import Transform.ETL_T_FX_RATE_LOAD;
import Transform.ETL_T_LOAN_COLLATERAL_LOAD;
import Transform.ETL_T_LOAN_DETAIL_LOAD;
import Transform.ETL_T_LOAN_GUARANTOR_LOAD;
import Transform.ETL_T_LOAN_LOAD;
import Transform.ETL_T_LOAN_MASTER_LOAD;
import Transform.ETL_T_PARTY_ACCOUNT_REL;
import Transform.ETL_T_PARTY_ADDRESS;
import Transform.ETL_T_PARTY_EMAIL;
import Transform.ETL_T_PARTY_NATIONALITY;
import Transform.ETL_T_PARTY_PARTY_REL;
import Transform.ETL_T_PARTY_PHONE;
import Transform.ETL_T_PARTY;
import Transform.ETL_T_SERVICE_LOAD;
import Transform.ETL_T_TRANSACTION_LOAD;
import Transform.ETL_T_TRANSFER_LOAD;
import tw.com.pershing.databean.ETLresponse;

@Path("/Tfunction")
public class Tfunction {
	
	// test : http://localhost:8083/AML_ETL/rest/Tfunction/WS1

	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public ETLresponse callTfunction(
			@QueryParam("filePath") String filePath,
//			@QueryParam("fileTypeName") String fileTypeName,
			@QueryParam("batch_no") String batch_no,
			@QueryParam("exc_central_no") String exc_central_no,
			@QueryParam("exc_record_date") String exc_record_dateStr,
			@QueryParam("upload_no") String upload_no/*,
			@QueryParam("program_no") String program_no */) {
		
		// Web Service回傳訊息
		ETLresponse response = new ETLresponse();
		// 進行中步驟
		String stepStr = "";
		// 程式代號
		String program_no = "";
		
		try {
			System.out.println(filePath);// for test
			
			/** 傳入參數基本檢核   **/
			stepStr = "\"傳入參數檢核\"";
			filePath = URLDecoder.decode(filePath, "UTF-8").trim();
			// 檢核傳入參數, 若有錯誤 throw相關錯誤訊息Exception
			new CheckParameter().check(filePath, batch_no, exc_central_no, exc_record_dateStr, upload_no);
			Date exc_record_date = new Date();
			exc_record_date = ETL_Tool_StringX.toUtilDate(exc_record_dateStr);
			
			ETL_Bean_LogData logData = new ETL_Bean_LogData();
			logData.setBATCH_NO(batch_no);
			logData.setCENTRAL_NO(exc_central_no);
			logData.setFILE_TYPE(null);
			logData.setRECORD_DATE(exc_record_date);
			logData.setUPLOAD_NO(upload_no);
			
			
			// 執行20支T系列程式
			stepStr = "ETL_T_ACCOUNT_PROPERTY";
			logData.setPROGRAM_NO("ETL_T_ACCOUNT_PROPERTY");
			new ETL_T_ACCOUNT_PROPERTY().trans_to_ACCOUNT_PROPERTY_LOAD(logData);

			stepStr = "ETL_T_ACCOUNT";
			logData.setPROGRAM_NO("ETL_T_ACCOUNT");
			new ETL_T_ACCOUNT().trans_to_ACCOUNT_LOAD(logData);

			stepStr = "ETL_T_BALANCE";
			logData.setPROGRAM_NO("ETL_T_BALANCE");
			new ETL_T_BALANCE().trans_to_BALANCE_LOAD(logData);

			stepStr = "ETL_T_CALENDAR_LOAD";
			logData.setPROGRAM_NO("ETL_T_CALENDAR_LOAD");
			new ETL_T_CALENDAR_LOAD().trans_to_CALENDAR_LOAD(logData);

			stepStr = "ETL_T_FX_RATE_LOAD";
			logData.setPROGRAM_NO("ETL_T_FX_RATE_LOAD");
			new ETL_T_FX_RATE_LOAD().trans_to_FX_RATE_LOAD(logData);

			stepStr = "ETL_T_LOAN_COLLATERAL_LOAD";
			logData.setPROGRAM_NO("ETL_T_LOAN_COLLATERAL_LOAD");
			new ETL_T_LOAN_COLLATERAL_LOAD().trans_to_LOAN_COLLATERAL_LOAD(logData);

			stepStr = "ETL_T_LOAN_DETAIL_LOAD";
			logData.setPROGRAM_NO("ETL_T_LOAN_DETAIL_LOAD");
			new ETL_T_LOAN_DETAIL_LOAD().trans_to_LOAN_DETAIL_LOAD(logData);

			stepStr = "ETL_T_LOAN_GUARANTOR_LOAD";
			logData.setPROGRAM_NO("ETL_T_LOAN_GUARANTOR_LOAD");
			new ETL_T_LOAN_GUARANTOR_LOAD().trans_to_GUARANTOR_LOAD(logData);

			stepStr = "ETL_T_LOAN_LOAD";
			logData.setPROGRAM_NO("ETL_T_LOAN_LOAD");
			new ETL_T_LOAN_LOAD().trans_to_LOAN_LOAD(logData);

			stepStr = "ETL_T_LOAN_MASTER_LOAD";
			logData.setPROGRAM_NO("ETL_T_LOAN_MASTER_LOAD");
			new ETL_T_LOAN_MASTER_LOAD().trans_to_LOAN_MASTER_LOAD(logData);

			stepStr = "ETL_T_PARTY_ACCOUNT_REL";
			logData.setPROGRAM_NO("ETL_T_PARTY_ACCOUNT_REL");
			new ETL_T_PARTY_ACCOUNT_REL().trans_to_PARTY_ACCOUNT_REL_LOAD(logData);

			stepStr = "ETL_T_PARTY_ADDRESS";
			logData.setPROGRAM_NO("ETL_T_PARTY_ADDRESS");
			new ETL_T_PARTY_ADDRESS().trans_to_PARTY_ADDRESS_LOAD(logData);

			stepStr = "ETL_T_PARTY_EMAIL";
			logData.setPROGRAM_NO("ETL_T_PARTY_EMAIL");
			new ETL_T_PARTY_EMAIL().trans_to_PARTY_EMAIL_LOAD(logData);

			stepStr = "ETL_T_PARTY_NATIONALITY";
			logData.setPROGRAM_NO("ETL_T_PARTY_NATIONALITY");
			new ETL_T_PARTY_NATIONALITY().trans_to_PARTY_NATIONALITY_LOAD(logData);

			stepStr = "ETL_T_PARTY_PARTY_REL";
			logData.setPROGRAM_NO("ETL_T_PARTY_PARTY_REL");
			new ETL_T_PARTY_PARTY_REL().trans_to_PARTY_PARTY_REL_LOAD(logData);

			stepStr = "ETL_T_PARTY_PHONE";
			logData.setPROGRAM_NO("ETL_T_PARTY_PHONE");
			new ETL_T_PARTY_PHONE().trans_to_PARTY_PHONE_LOAD(logData);

			stepStr = "ETL_T_PARTY";
			logData.setPROGRAM_NO("ETL_T_PARTY");
			new ETL_T_PARTY().trans_to_PARTY_LOAD(logData);

			stepStr = "ETL_T_SERVICE_LOAD";
			logData.setPROGRAM_NO("ETL_T_SERVICE_LOAD");
			new ETL_T_SERVICE_LOAD().trans_to_SERVICE_LOAD(logData);

			stepStr = "ETL_T_TRANSACTION_LOAD";
			logData.setPROGRAM_NO("ETL_T_TRANSACTION_LOAD");
			new ETL_T_TRANSACTION_LOAD().trans_to_TRANSACTION_LOAD(logData);

			stepStr = "ETL_T_TRANSFER_LOAD";
			logData.setPROGRAM_NO("ETL_T_TRANSFER_LOAD");
			new ETL_T_TRANSFER_LOAD().trans_to_TRANSFER_LOAD(logData);
			
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
			response.setError("執行" + stepStr + "發生錯誤:" + ex.getMessage());
		}
		
		return response;
	}
	
}
