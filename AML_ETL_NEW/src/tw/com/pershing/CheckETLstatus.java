package tw.com.pershing;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import tw.com.pershing.databean.ETLresponse;

@Path("/checkETLstatus")
public class CheckETLstatus {
	
	// test : http://localhost:8083/AML_ETL/rest/checkETLstatus/WS1?testInput=check
	
	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public static ETLresponse checkETLstatus(@QueryParam("testInput") String testInput) {
		
		ETLresponse response = new ETLresponse();
		
		if ("check".equals(testInput)) {
			response.setMsg("SUCCESS");
		} else {
			response.setMsg("FAILURE");
		}
		
		return response;
		
	}

}
