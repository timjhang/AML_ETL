package tw.com.pershing;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import Tool.ETL_Tool_RelevanceSql_To_Excels;
import tw.com.pershing.databean.ETLresponse;

@Path("/relevance")
public class Relevance {

	// test : http://localhost:8080/AML_ETL/rest/relevance/WS1?BatchNo=ETL00001&CentralNo=600&RecodeDate=2018-09-07
	
	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public ETLresponse relevanceCheck(
			@QueryParam("BatchNo") String batchNo,
			@QueryParam("CentralNo") String centralNo,
			@QueryParam("RecodeDate") String recordDate) {
		
		// Web Service回傳訊息
		ETLresponse response = new ETLresponse();
		
		try {
		
//			ETL_Tool_RelevanceSql_To_Excels.execute(batchNo, centralNo, recordDate);
			new ETL_Tool_RelevanceSql_To_Excels(batchNo, recordDate).execute(centralNo);
			
			response.setMsg("SUCCESS");
		
		} catch (Exception ex) {
			ex.printStackTrace();
			response.setMsg("FAILURE");
			response.setError(ex.getMessage());
		}
		
		return response;
	}
	
}
