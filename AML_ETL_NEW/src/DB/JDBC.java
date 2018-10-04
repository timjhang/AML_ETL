package DB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import Bean.ETL_DETAIL_LOG;
import Bean.ETL_FILE_LOG;
import Profile.ETL_Profile;

public class JDBC {

	private String insert_etl_file_log = " INSERT INTO " + ETL_Profile.db2TableSchema + ".ETL_FILE_LOG ( "
			+ " BATCH_NO, " 
			+ " CENTRAL_NO, " 
			+ " RECORD_DATE, " 
			+ " FILE_TYPE, " 
			+ " FILE_NAME, " 
			+ " UPLOAD_NO, "
			+ " STEP_TYPE, " 
			+ " START_DATETIME, " 
			+ " END_DATETIME, " 
			+ " TOTAL_CNT, " 
			+ " SUCCESS_CNT, "
			+ " FAILED_CNT, " 
			+ " EXE_RESULT, " 
			+ " EXE_RESULT_DESCRIPTION, " 
			+ " SRC_FILE "
			+ ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private String insert_etl_detail_log = " INSERT INTO " + ETL_Profile.db2TableSchema + ".ETL_DETAIL_LOG ( "
			+ " BATCH_NO, " 
			+ " CENTRAL_NO, " 
			+ " RECORD_DATE, " 
			+ " UPLOAD_NO, "
			+ " STEP_TYPE, " 
			+ " PROGRAM_NO, " 
			+ " EXE_STATUS, " 
			+ " EXE_RESULT, " 
			+ " EXE_RESULT_DESCRIPTION, " 
			+ " START_DATETIME" 
			+ ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private String update_etl_file_log = " UPDATE " + ETL_Profile.db2TableSchema
			+ ".ETL_FILE_LOG SET "
			+ " EXE_RESULT = ?,"
			+ " EXE_RESULT_DESCRIPTION = ?,"
			+ " END_DATETIME = ?"
			+ " WHERE BATCH_NO = ?"
			+ " AND CENTRAL_NO = ?"
			+ " AND RECORD_DATE = ?"
			+ " AND FILE_TYPE = ?"
			+ " AND FILE_NAME = ?"
			+ " AND UPLOAD_NO = ?"
			+ " AND STEP_TYPE = ?"
			+ " AND TOTAL_CNT = ?"
			+ " AND SUCCESS_CNT = ?"
			+ " AND FAILED_CNT = ?"
			+ " AND SRC_FILE = ?";

	private String update_etl_detail_log = " UPDATE " + ETL_Profile.db2TableSchema
			+ ".ETL_DETAIL_LOG SET "
			+ " EXE_STATUS = ?,"
			+ " EXE_RESULT = ?,"
			+ " EXE_RESULT_DESCRIPTION = ?,"
			+ " END_DATETIME = ?"
			+ " WHERE BATCH_NO = ?"
			+ " AND CENTRAL_NO = ?"
			+ " AND RECORD_DATE = ?"
			+ " AND UPLOAD_NO = ?"
			+ " AND STEP_TYPE = ?"
			+ " AND PROGRAM_NO = ?";
	
	public void insertLog(ETL_DETAIL_LOG bean) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {

			conn = ConnectionHelper.getDB2Connection();

			stmt = conn.prepareStatement(insert_etl_detail_log);

			stmt.setString(1, bean.getBatch_no());
			stmt.setString(2, bean.getCentral_no());
			stmt.setDate(3, bean.getRecord_date());
			stmt.setString(4, "001");
			stmt.setString(5, "R");
			stmt.setString(6, "AML_ETL_RelevanceSql_To_Excels");
			stmt.setString(7, "S");
			stmt.setString(8, "");
			stmt.setString(9, "執行中");
			stmt.setTimestamp(10, new Timestamp(System.currentTimeMillis()));

			stmt.executeUpdate();

			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
				se2.printStackTrace();
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
	
	public void insertLog(ETL_FILE_LOG bean) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {

			conn = ConnectionHelper.getDB2Connection();

			stmt = conn.prepareStatement(insert_etl_file_log);

			stmt.setString(1, bean.getBatch_no());
			stmt.setString(2, bean.getCentral_no());
			stmt.setDate(3, bean.getRecord_date());
			stmt.setString(4, "");
			stmt.setString(5, bean.getFile_name());
			stmt.setString(6, "001");
			stmt.setString(7, "R");
			stmt.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
			stmt.setTimestamp(9, null);
			stmt.setInt(10, 0);
			stmt.setInt(11, 0);
			stmt.setInt(12, 0);
			stmt.setString(13, bean.getExe_result());
			stmt.setString(14, bean.getExe_result_description());
			stmt.setString(15, bean.getSrc_file());

			stmt.executeUpdate();

			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
				se2.printStackTrace();
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	public void updateLog(ETL_FILE_LOG bean) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {

			conn = ConnectionHelper.getDB2Connection();

			stmt = conn.prepareStatement(update_etl_file_log);
			
			stmt.setString(1, bean.getExe_result());
			stmt.setString(2, bean.getExe_result_description());
			stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			stmt.setString(4, bean.getBatch_no());
			stmt.setString(5, bean.getCentral_no());
			stmt.setDate(6, bean.getRecord_date());
			stmt.setString(7, "");
			stmt.setString(8, bean.getFile_name());
			stmt.setString(9, "001");
			stmt.setString(10, "R");
			stmt.setInt(11, 0);
			stmt.setInt(12, 0);
			stmt.setInt(13, 0);
			stmt.setString(14, bean.getSrc_file());
			
			stmt.executeUpdate();

			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
				se2.printStackTrace();
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
	
	public void updateLog(ETL_DETAIL_LOG bean) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {

			conn = ConnectionHelper.getDB2Connection();

			stmt = conn.prepareStatement(update_etl_detail_log);
			
			stmt.setString(1, bean.getExe_status());
			stmt.setString(2, bean.getExe_result());
			stmt.setString(3, bean.getExe_result_description());
			stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
			stmt.setString(5, bean.getBatch_no());
			stmt.setString(6, bean.getCentral_no());
			stmt.setDate(7, bean.getRecord_date());
			stmt.setString(8, "001");
			stmt.setString(9, "R");
			stmt.setString(10, "AML_ETL_RelevanceSql_To_Excels");
			
			stmt.executeUpdate();

			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
				se2.printStackTrace();
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
}
