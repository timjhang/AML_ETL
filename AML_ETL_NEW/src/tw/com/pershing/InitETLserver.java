package tw.com.pershing;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import DB.ConnectionHelper;
import Profile.ETL_Profile;
import tw.com.pershing.databean.ETLresponse;

@Path("/initETLserver")
public class InitETLserver {
	
	// test : http://localhost:8080/AML_ETL/rest/initETLserver/WS1
	
	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public ETLresponse initETLServer(@QueryParam("action") String action) {
		
		// Web Service回傳訊息
		ETLresponse response = new ETLresponse();
		
		try {
		
			if (!"initial".equals(action)) {
				response.setMsg("FAILURE");
				response.setError("啟動命令action 錯誤: " + action + " " + new Date());
				
				return response;
			}
			
			System.out.println("###### InitETLserver  Start " + new Date());
			
			// 清除ETL Server上使用Table
			clearTables();
			
			System.out.println("###### InitETLserver  End " + new Date());
			
			response.setMsg("SUCCESS");
		
		} catch (Exception ex) {
			ex.printStackTrace();
			response.setMsg("FAILURE");
			response.setError(ex.getMessage());
		}
		
		return response;
	}
	
	// 清除E系列tables(temp_temp, temp), T系列tables(load), error_log, 並重新reorg
	private void clearTables() throws Exception {
		
		System.out.println("#######InitETLserver - clearTables - Start");
		
		String sql = "{call " + ETL_Profile.db2TableSchema + ".ETL_SERVER_SERVICE.clearETtables(?,?)}";
		
		Connection con = ConnectionHelper.getDB2Connection();
		CallableStatement cstmt = con.prepareCall(sql);
		
		cstmt.registerOutParameter(1, Types.INTEGER);
		cstmt.registerOutParameter(2, Types.VARCHAR);
		
		cstmt.execute();
		
		int returnCode = cstmt.getInt(1);
		
		if (returnCode != 0) {
			String errorMessage = cstmt.getString(2);
//	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
			
		System.out.println("#######InitETLserver - clearTables - End");
		
	}

}
