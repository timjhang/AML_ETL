package ErrorFile;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.ibm.db2.jcc.DB2Types;

import Bean.ETL_Bean_ERROR_LOG_DOWNLOAD;
import Bean.ETL_Bean_ERROR_LOG_DOWNLOAD_FILE;
import DB.ConnectionHelper;

import Profile.ETL_Profile;

public class ErrorFileDAO {

	// 取得 ETL_Bean_ERROR_LOG_DOWNLOAD
	private static List<ETL_Bean_ERROR_LOG_DOWNLOAD> getErrorLogDownload() throws Exception {
		List<ETL_Bean_ERROR_LOG_DOWNLOAD> resultList = new ArrayList<ETL_Bean_ERROR_LOG_DOWNLOAD>();

		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;

		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".ERROR_FILE.ETL_Bean_ERROR_LOG_DOWNLOAD(?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, DB2Types.CURSOR);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			// (java.sql.ResultSet)
			rs = (java.sql.ResultSet) cstmt.getObject(2);

			if (rs == null) {
				return resultList;
			}

			while (rs.next()) {
				ETL_Bean_ERROR_LOG_DOWNLOAD obj = new ETL_Bean_ERROR_LOG_DOWNLOAD();
				obj.setError_description(rs.getString("ERROR_DESCRIPTION"));
				obj.setField_name(rs.getString("FIELD_NAME"));
				obj.setFile_name(rs.getString("FILE_NAME"));
				obj.setMark_1(rs.getString("MARK_1"));
				obj.setMark_2(rs.getString("MARK_2"));
				obj.setMark_3(rs.getString("MARK_3"));
				obj.setRow_count(rs.getString("ROW_COUNT"));
				obj.setSrc_data(rs.getString("SRC_DATA"));
				resultList.add(obj);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException e) {
//				e.printStackTrace();
			}
		}

		return resultList;
	}

	// 取得 ETL_Bean_ERROR_LOG_DOWNLOAD_FILES
	public static List<ETL_Bean_ERROR_LOG_DOWNLOAD_FILE> getErrorLogDownloadFile() throws Exception {
		List<ETL_Bean_ERROR_LOG_DOWNLOAD_FILE> resultList = new ArrayList<ETL_Bean_ERROR_LOG_DOWNLOAD_FILE>();
		List<ETL_Bean_ERROR_LOG_DOWNLOAD> allBody = ErrorFileDAO.getErrorLogDownload();

		ETL_Bean_ERROR_LOG_DOWNLOAD_FILE file = new ETL_Bean_ERROR_LOG_DOWNLOAD_FILE();

		for (int i = 0; i < allBody.size(); i++) {
			ETL_Bean_ERROR_LOG_DOWNLOAD row = allBody.get(i);

			// 如果說名稱為檔案名稱為 null 或者 File 檔案名稱與row檔案名稱相同 加入 file body
			if (file.getFile_name() == null)
				file.setFile_name(row.getFile_name());

			if (!file.getFile_name().equals(row.getFile_name())) {
				// 如果說不相等 現有file 加入 回傳的list
				resultList.add(file);
				file = new ETL_Bean_ERROR_LOG_DOWNLOAD_FILE();
				file.setFile_name(row.getFile_name());
			}

			file.getBody().add(row);

			// 最後一個也要加入resultList
			if (i == (allBody.size() - 1)) {
				resultList.add(file);
			}

		}

		return resultList;
	}
	
	// 清除 ERROR_LOG_DOWNLOAD
	public static boolean clear_table_ERROR_LOG_DOWNLOAD() {
		CallableStatement cstmt = null;
		Connection con = null;

		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".ERROR_FILE.clear_table_ERROR_LOG_DOWNLOAD(?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
			System.out.println("ErrorFileDAO -- clear_table_ERROR_LOG_DOWNLOAD  執行成功！");
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("ErrorFileDAO -- clear_table_ERROR_LOG_DOWNLOAD  執行失敗！");
			
			return false;
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
//				e.printStackTrace();
			}
		}
	}
	
	// Error Log 寫入 ERROR_LOG_DOWNLOAD
	public static void write_ERROR_LOG_DOWNLOAD() {
		CallableStatement cstmt = null;
		Connection con = null;

		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".ERROR_FILE.write_ERROR_LOG_DOWNLOAD(?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
			System.out.println("ErrorFileDAO -- write_ERROR_LOG_DOWNLOAD  執行成功！");

		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("ErrorFileDAO -- write_ERROR_LOG_DOWNLOAD  執行失敗！");
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
//					e.printStackTrace();
			}
		}
	}

	public static void main(String args[]) {
		System.out.println("開始");

	}
}
