package DB;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import Bean.ETL_Bean_Response;
import Profile.ETL_Profile;

public class ETL_P_DMData_Writer {

	// truncate table
	public static boolean truncateMappingDataTable(String tableName, String central_No) {
		boolean isSuccess = false;
		Connection con = null;
		CallableStatement cstmt = null;
		String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".DM.truncateTable(?,?,?,?)}";

		try {
			con = ConnectionHelper.getDB2ConnGAML(central_No);

			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, ETL_Profile.GAML_db2TableSchema);
			cstmt.setString(3, tableName);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			} else {
				isSuccess = true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isSuccess = false;
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		if (isSuccess) {
			sql = "{call " + ETL_Profile.db2TableSchema + ".DM.truncateTable(?,?,?,?)}";
			try {
				con = ConnectionHelper.getDB2Connection();

				cstmt = con.prepareCall(sql);

				cstmt.registerOutParameter(1, Types.INTEGER);
				cstmt.setString(2, ETL_Profile.db2TableSchema);
				cstmt.setString(3, tableName);
				cstmt.registerOutParameter(4, Types.VARCHAR);
				cstmt.execute();

				int returnCode = cstmt.getInt(1);

				if (returnCode != 0) {
					String errorMessage = cstmt.getString(4);
					System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				} else {
					isSuccess = true;
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				isSuccess = false;
			} finally {
				try {
					// 資源後開,先關
					if (cstmt != null) {
						cstmt.close();
					}
					if (con != null) {
						con.close();
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

		return isSuccess;
	}

	// truncate table
	public static boolean truncateTable(String tableName) {
		boolean isSuccess = false;
		Connection con = null;
		CallableStatement cstmt = null;
		String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.truncateTable(?,?,?,?)}";

		try {
			con = ConnectionHelper.getDB2Connection();

			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, ETL_Profile.db2TableSchema);
			cstmt.setString(3, "ERROR_LOG");
			cstmt.registerOutParameter(4, Types.VARCHAR);
			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			} else {
				isSuccess = true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isSuccess = false;
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		return isSuccess;
	}

	// checkData
	public static ETL_Bean_Response checkAcctmappingData(String central_no, String fileName) throws Exception {

		// -- 將pk重複的updata errorMark = Y v_DataStatus1Count 舊資料重複 =1 v_DataStatus2Count
		// 整筆資料重複 = 2
		// PROCEDURE checkAcctmappingData(v_returnCode out INT,v_DataStatus out
		// INT,v_SuccessCount out INT, v_ErrorMsg out VARCHAR)
		ETL_Bean_Response response = new ETL_Bean_Response();

		String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.checkAcctmappingData(?,?,?,?,?,?)}";

		Connection con = ConnectionHelper.getDB2Connection();

		CallableStatement cstmt = con.prepareCall(sql);

		cstmt.registerOutParameter(1, Types.INTEGER); // v_returnCode
		cstmt.setString(2, central_no); // v_central_no
		cstmt.setString(3, fileName); // v_fileName
		cstmt.registerOutParameter(4, Types.INTEGER); // v_DataStatus
		cstmt.registerOutParameter(5, Types.INTEGER); // v_SuccessCount
		cstmt.registerOutParameter(6, Types.VARCHAR); // v_ErrorMsg

		cstmt.execute();

		int returnCode = cstmt.getInt(1);

		if (returnCode != 0) {
			String errorMessage = cstmt.getString(6);
			System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception(errorMessage);
		} else {
			Integer dataStatus = cstmt.getInt(4);
			Integer successCount = cstmt.getInt(5);

			response.setDataStatus(dataStatus);
			response.setSuccessCount(successCount);
			response.setSuccess(true);
			return response;

		}

	}

	// checkData
	public static ETL_Bean_Response checkBranchmappingData(String central_no, String fileName) throws Exception {

		// -- 將pk重複的updata errorMark = Y v_DataStatus1Count 舊資料重複 =1 v_DataStatus2Count
		// 整筆資料重複 = 2
		// PROCEDURE checkAcctmappingData(v_returnCode out INT,v_DataStatus out
		// INT,v_SuccessCount out INT, v_ErrorMsg out VARCHAR)
		ETL_Bean_Response response = new ETL_Bean_Response();

		String sql = "{call " + ETL_Profile.db2TableSchema + ".DM.checkBranchmappingData(?,?,?,?,?,?)}";

		Connection con = ConnectionHelper.getDB2Connection();

		CallableStatement cstmt = con.prepareCall(sql);

		cstmt.registerOutParameter(1, Types.INTEGER); // v_returnCode
		cstmt.setString(2, central_no); // v_central_no
		cstmt.setString(3, fileName); // v_fileName
		cstmt.registerOutParameter(4, Types.INTEGER); // v_DataStatus
		cstmt.registerOutParameter(5, Types.INTEGER); // v_SuccessCount
		cstmt.registerOutParameter(6, Types.VARCHAR); // v_ErrorMsg

		cstmt.execute();

		int returnCode = cstmt.getInt(1);

		if (returnCode != 0) {
			String errorMessage = cstmt.getString(6);
			System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception(errorMessage);
		} else {
			Integer dataStatus = cstmt.getInt(4);
			Integer successCount = cstmt.getInt(5);

			response.setDataStatus(dataStatus);
			response.setSuccessCount(successCount);
			response.setSuccess(true);
			return response;

		}

	}

	public static void main(String[] argv) {

		try {
			boolean isSuccess;

			// isSuccess = truncateMappingDataTable("party7", "600");

			// System.out.println(truncateErrorLogTable());
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
}
