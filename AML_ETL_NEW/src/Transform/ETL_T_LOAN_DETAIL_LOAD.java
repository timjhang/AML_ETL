package Transform;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_T_LOAN_DETAIL_LOAD extends Transform {
	
	public ETL_T_LOAN_DETAIL_LOAD() {
		
	}

	public ETL_T_LOAN_DETAIL_LOAD(ETL_Bean_LogData logData) {
		super(logData);
	}

	@Override
	public void trans_File() {
		try {
			trans_to_LOAN_DETAIL_LOAD(this.logData);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	// 觸發DB2轉換Procedure, 轉換資料寫進LOAN_DETAIL_LOAD
	public void trans_to_LOAN_DETAIL_LOAD(ETL_Bean_LogData logData) {
		
		System.out.println("#######Transform - ETL_T_LOAN_DETAIL - Start");
		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
			
			// TODO
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Transform.TempTo_LOAN_DETAIL_LOAD(?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
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

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		System.out.println("#######Transform - ETL_T_LOAN_DETAIL - End");
		
	}
}
