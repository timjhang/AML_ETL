package tw.com.pershing;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import ControlWS.ETL_C_GET_UPLOAD_FILE;
import tw.com.pershing.databean.ETLresponse;

@Path("/getUploadFile")
public class GetUploadFile {
	
	// test : http://localhost:8083/AML_ETL/rest/getUploadFile/WS1?centralNo=600
	
	@GET
	@Path("/WS1")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public static ETLresponse getUploadFile(@QueryParam("centralNo") String centralNo,
											@QueryParam("runType") String runType,
											@QueryParam("rerunRecordDate") String rerunRecordDateStr) {
		
		ETLresponse response = new ETLresponse();
		
		try {
			
			String[] downloadFileInfo = new String[1];
			
			if (!"RERUN".equals(runType)) {
				if (ETL_C_GET_UPLOAD_FILE.download_SFTP_Files(centralNo, downloadFileInfo)) {
					response.setFileInfo(downloadFileInfo[0]);
					response.setMsg("SUCCESS");
				} else {
					response.setMsg("FAILURE");
				}
			} else {
				if (ETL_C_GET_UPLOAD_FILE.download_SFTP_RerunFiles(centralNo, downloadFileInfo, rerunRecordDateStr)) {
					response.setFileInfo(downloadFileInfo[0]);
					response.setMsg("SUCCESS");
				} else {
					response.setMsg("FAILURE");
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			response.setMsg("Exception");
			response.setError(ex.getMessage());
		}
		
		return response;
	}
	
	@GET
	@Path("/WS2")
	@Produces (MediaType.APPLICATION_XML + ";charset=UTF-8")
	public static ETLresponse getMigrationUploadFile(@QueryParam("centralNo") String centralNo) {
		
		ETLresponse response = new ETLresponse();
		
		try {
			
			String[] downloadFileInfo = new String[1];
			
			if (ETL_C_GET_UPLOAD_FILE.download_SFTP_MigrationFiles(centralNo, downloadFileInfo)) {
				response.setFileInfo(downloadFileInfo[0]);
				response.setMsg("SUCCESS");
			} else {
				response.setMsg("FAILURE");
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			response.setMsg("Exception");
			response.setError(ex.getMessage());
		}
		
		return response;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
