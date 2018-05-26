package tw.com.pershing;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import Profile.ETL_Profile;
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
			
			try {
				InetAddress addr = InetAddress.getLocalHost();    
		  		String ipAddr = addr.getHostAddress(); // Get IP Address

		  		
		  		System.out.println("Profile Url = " + ETL_Profile.db2Url);
		  		System.out.println("ipAddr = " + ipAddr);
		  		
		  		if (isLocalHostIpMatch(ETL_Profile.db2Url)) {
		  			response.setMsg("SUCCESS");
		  		} else {
		  			response.setMsg("FAILURE");
		  		}
		  		
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
		} else {
			response.setMsg("FAILURE");
		}
		
		return response;
	}
	
	public static boolean isLocalHostIpMatch(String etlServerUrl) throws SocketException {
		
		Enumeration allNetInterfaces=NetworkInterface.getNetworkInterfaces(); 
		InetAddress ip=null; 
		while(allNetInterfaces.hasMoreElements()) { 
			NetworkInterface netInterface=(NetworkInterface) allNetInterfaces.nextElement(); 
			//System.out.println(netInterface.getName()); 
			Enumeration addresses=netInterface.getInetAddresses(); 
			while(addresses.hasMoreElements()){  
				ip = (InetAddress) addresses.nextElement(); 
				if (ip != null && ip instanceof Inet4Address) { 
					System.out.println("本機的ip = " + ip.getHostAddress()); 
					
					if (etlServerUrl.contains(ip.getHostAddress())) {
						return true;
					}
				} 
			} 
		} 
		return false; 
	}
	
	public static void main(String[] argv) {
		try {
			System.out.println(ETL_Profile.db2Url);
			
			InetAddress addr = InetAddress.getLocalHost();    
	  		String ipAddr = addr.getHostAddress(); // Get IP Address
	  		System.out.println(ipAddr);
	  		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
