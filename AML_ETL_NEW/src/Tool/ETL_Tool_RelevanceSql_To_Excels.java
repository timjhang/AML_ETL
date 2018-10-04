package Tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import Bean.ETL_DETAIL_LOG;
import Bean.ETL_FILE_LOG;
import DB.ConnectionHelper;
import DB.JDBC;
import Profile.SQLQUERY_Profile;

public class ETL_Tool_RelevanceSql_To_Excels {

	private int pageMaxRowCount = 65534;
	private String record_date = null;
	private String central_No = null;;
	private String currentQuery = null;
	private String savePath = null;
	private ArrayList<String> columnNames = null;
	private static Map<String, Integer> dataMap = null;

	public ETL_Tool_RelevanceSql_To_Excels() {
	}

	public ETL_Tool_RelevanceSql_To_Excels(String central_No, String record_date) {
		this.central_No = central_No;
		this.record_date = record_date;
		this.savePath = "D:" + File.separator + "DataCheck" + File.separator + central_No;
	}

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		long time1, time2;

		time1 = System.currentTimeMillis();

		boolean success = ETL_Tool_RelevanceSql_To_Excels.execute("ETRS01", "952", "2018-07-05");
//		boolean success = ETL_Tool_RelevanceSql_To_Excels.execute(args[0], args[1], args[2]);

		time2 = System.currentTimeMillis();

		System.out.println("執行結果" + (success ? "成功" : "失敗") + "/花了：" + (time2 - time1) + "豪秒");
	}

	public static boolean execute(String batch_no, String central_no, String record_date)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String[] centralNoarr = central_no.split(",");
		String[] querys = SQLQUERY_Profile.getQuerys();
		JDBC jdbc = null;
		ETL_FILE_LOG fileLog = null;
		ETL_DETAIL_LOG detailLog = null;
		ETL_Tool_RelevanceSql_To_Excels etl_Tool_RelevanceSql_To_Excels = null;

		boolean isSuccess = false;

		try {
			jdbc = new JDBC();

			for (String centralNo : centralNoarr) {
				System.out.println("\n執行單位:" + centralNo);

				java.sql.Date recordDate = new java.sql.Date(
						new SimpleDateFormat("yyyy-MM-dd").parse(record_date).getTime());

				detailLog = new ETL_DETAIL_LOG();
				detailLog.setBatch_no(batch_no);
				detailLog.setCentral_no(centralNo);
				detailLog.setRecord_date(recordDate);
				detailLog.setExe_status("S");
				detailLog.setExe_result("");
				detailLog.setExe_result_description("執行中");

				jdbc.insertLog(detailLog);

				try {
					etl_Tool_RelevanceSql_To_Excels = new ETL_Tool_RelevanceSql_To_Excels(centralNo, record_date);

					dataMap = new HashMap<String, Integer>();

					for (String query : querys) {

						fileLog = new ETL_FILE_LOG();
						fileLog.setBatch_no(batch_no);
						fileLog.setCentral_no(centralNo);
						fileLog.setRecord_date(recordDate);
						fileLog.setFile_name(SQLQUERY_Profile.getFileName(query));
						fileLog.setExe_result("");
						fileLog.setExe_result_description("執行中");
						fileLog.setSrc_file(SQLQUERY_Profile.getSrcFile(query));
						jdbc.insertLog(fileLog);

						try {
							ResultSet rs = etl_Tool_RelevanceSql_To_Excels.getResultsetFromSql(query);
							etl_Tool_RelevanceSql_To_Excels
									.generateExcel(etl_Tool_RelevanceSql_To_Excels.processResultSet(rs));

							fileLog.setExe_result("Y");
							fileLog.setExe_result_description("無錯誤資料");
							jdbc.updateLog(fileLog);
						} catch (Exception e) {

							fileLog.setExe_result("S");
							fileLog.setExe_result_description(e.toString().substring(0, 400));
							jdbc.updateLog(fileLog);
						}

					}

					detailLog.setExe_status("E");
					detailLog.setExe_result("Y");
					detailLog.setExe_result_description("無錯誤資料");

					jdbc.updateLog(detailLog);
				} catch (Exception e) {

					detailLog.setExe_status("E");
					detailLog.setExe_result("S");
					detailLog.setExe_result_description(e.toString().substring(0, 400));
					jdbc.updateLog(detailLog);
				}

				etl_Tool_RelevanceSql_To_Excels.generateDataCheckExcel();
				System.out.println(centralNo + "關連檔產生完成");
			}
			isSuccess = true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return isSuccess;
	}

	/**
	 * This method returns a ResultSet for the given sql query.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public ResultSet getResultsetFromSql(String sql)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// Connection conn = getDBConnection();
		this.currentQuery = sql;
		Connection conn = ConnectionHelper.getDB2ConnGAML(central_No);
		ResultSet rs = null;
		try {
			// Statement stmt = conn.createStatement();
			PreparedStatement pstmt = conn.prepareStatement(sql);

			int pstmtNum = SQLQUERY_Profile.getPreparedStatementNum(currentQuery);
			for (int i = 1; i <= pstmtNum; i++) {
				pstmt.setString(i, record_date);
			}
			rs = pstmt.executeQuery();
			// rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * This method returns a Map with keys as row numbers and values as another
	 * LinkedHashMap containing key as column name and value as column value ,
	 * present in the ResulSet. We have used LinkedHashMap because it maintains
	 * the order in which the values are put in the Map.
	 */
	public Map<String, LinkedHashMap<String, String>> processResultSet(ResultSet rs) {
		columnNames = new ArrayList<String>();
		LinkedHashMap<String, String> rowDetails = new LinkedHashMap<String, String>();
		Map<String, LinkedHashMap<String, String>> resultMap = new LinkedHashMap<String, LinkedHashMap<String, String>>();
		ResultSetMetaData rsm = null;

		if (rs != null) {
			try {
				rsm = (ResultSetMetaData) rs.getMetaData();
				for (int i = 1; i <= rsm.getColumnCount(); i++) {
					// System.out.println(i + " -> " + rsm.getColumnName(i));
					columnNames.add(rsm.getColumnName(i));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		try {
			int rowCount = 1;
			while (rs.next()) {
				for (int i = 1; i <= rsm.getColumnCount(); i++) {
					rowDetails.put(rsm.getColumnName(i), rs.getString(i));
				}
				resultMap.put(new Integer(rowCount).toString(), rowDetails);
				rowCount++;
				rowDetails = new LinkedHashMap<String, String>();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultMap;
	}

	public void generateDataCheckExcel() {
		FileOutputStream fileOut = null;
		SXSSFWorkbook wb = null;
		SXSSFRow row = null;

		try {
			wb = new SXSSFWorkbook();
			SXSSFSheet sheet = wb.createSheet("第1頁");

			CellStyle headerStyle = wb.createCellStyle();
			Font headerFont = wb.createFont();
			headerFont.setBold(true);
			headerStyle.setFont(headerFont);
			headerStyle.setAlignment(HorizontalAlignment.CENTER);

			CellStyle detailStyle = wb.createCellStyle();
			Font detailFont = wb.createFont();
			detailStyle.setFont(detailFont);
			detailStyle.setAlignment(HorizontalAlignment.CENTER);

			row = sheet.createRow(0);

			String[] cell0Arr = { "編號", "檔名", "未關聯筆數" };
			String[] sortOrder = SQLQUERY_Profile.getSortOrder();

			for (int i = 0; i < cell0Arr.length; i++) {
				SXSSFCell cell0 = row.createCell(i);
				cell0.setCellStyle(headerStyle);
				cell0.setCellValue(cell0Arr[i]);
			}

			for (int i = 1; i <= dataMap.size(); i++) {
				row = sheet.createRow(i);

			}

			int i = 1;

			for (String key : sortOrder) {
				SXSSFRow curr_row = sheet.createRow(i++);
				curr_row.createCell(0).setCellValue(i - 1);
				curr_row.getCell(0).setCellStyle(detailStyle);
				curr_row.createCell(1).setCellValue(key);
				curr_row.createCell(2).setCellValue(dataMap.get(key));
				curr_row.getCell(2).setCellStyle(detailStyle);
			}

			sheet.setColumnWidth(0, (int) ((50 + 0.72) * 85));
			sheet.setColumnWidth(1, (int) ((50 + 0.72) * 250));
			sheet.setColumnWidth(2, (int) ((50 + 0.72) * 85));

			String fileName = central_No + "_ETL傳檔資料關聯檢核總表_" + record_date.replaceAll("-", "");

			File file = new File(savePath + File.separator + central_No + "_ETL關聯檢查_" + record_date.replaceAll("-", "")
					+ File.separator + fileName + ".xlsx");
			if (file.getParentFile() != null) {
				file.getParentFile().mkdirs();
			}
			file.createNewFile();
			fileOut = new FileOutputStream(file);

			wb.write(fileOut);
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		} finally {

			try {
				if (wb != null) {
					wb.dispose();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				if (fileOut != null) {
					fileOut.flush();
					fileOut.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method generates an excel sheet containing data from the given Map.
	 * The name of the excel sheet will be the String passed as a parameter.
	 */
	public void generateExcel(Map<String, LinkedHashMap<String, String>> resultMap) {
		FileOutputStream fileOut = null;
		SXSSFWorkbook wb = null;
		String key = null;

		try {
			StringBuilder fileName = new StringBuilder(central_No);

			fileName = SQLQUERY_Profile.getFileNameDetail(fileName, currentQuery);

			key = fileName.toString().replaceFirst(central_No, "").replaceAll("_", "").replaceAll("[0-9]", "");

			fileName.append(record_date.replaceAll("-", ""));
			System.out.println("檔名:" + fileName);

			wb = new SXSSFWorkbook();
			CellStyle headerStyle = wb.createCellStyle();
			SXSSFSheet sheet3 = wb.createSheet("第1頁");

			Font headerFont = wb.createFont();
			headerFont.setBold(true);

			// headerFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
			//
			// headerStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
			// headerStyle.setFillForegroundColor(HSSFColor.PALE_BLUE.index);
			// headerStyle.setFillBackgroundColor(HSSFColor.WHITE.index);
			headerStyle.setFont(headerFont);
			headerStyle.setAlignment(HorizontalAlignment.CENTER);

			try {
				File file = new File(savePath + File.separator + central_No + "_ETL關聯檢查_"
						+ record_date.replaceAll("-", "") + File.separator + fileName + ".xlsx");
				if (file.getParentFile() != null) {
					file.getParentFile().mkdirs();
				}
				file.createNewFile();
				fileOut = new FileOutputStream(file);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			// HSSFRow sessionname = sheet3.createRow(2);
			// HSSFCell title = sessionname.createCell(3);
			// title.setCellStyle(headerStyle);
			// title.setCellValue(name);

			Map<String, LinkedHashMap<String, String>> rMap = resultMap;
			// Map<String, String> columnDetails = rMap.get("1");
			Map<String, String> columnDetails = null;
			Set<String> set = null;
			SXSSFRow row = null;

			System.out.println("資料" + rMap.size() + "筆");

			dataMap.put(key, rMap.size());

			if (rMap != null && rMap.size() > 0) {

				columnDetails = rMap.get("1");
				set = columnDetails.keySet();

				row = sheet3.createRow(0);

				int firstPageCellNum = 0;
				for (String s : set) {
					SXSSFCell cell0 = row.createCell(firstPageCellNum);
					cell0.setCellStyle(headerStyle);
					cell0.setCellValue(s);

					firstPageCellNum++;
				}

				// 分頁次數
				int index = 0;
				int firstPageMaxCount = pageMaxRowCount + 1;

				for (int i = 1; i <= rMap.size(); i++) {
					int cellNum = 0;

					columnDetails = rMap.get(new Integer(i).toString());

					// if (i % (index == 0 ? firstPageMaxCount :
					// pageMaxRowCount) == 0) {
					// sheet3 = wb.createSheet("" + (index + 2));
					// index++;
					//
					// }

					if (i % pageMaxRowCount == 0) {
						int addPageCellNum = 0;

						sheet3 = wb.createSheet("第" + (index + 2) + "頁");
						row = sheet3.createRow(0);

						for (String s : set) {
							SXSSFCell cell0 = row.createCell(addPageCellNum);
							cell0.setCellStyle(headerStyle);
							cell0.setCellValue(s);

							addPageCellNum++;
						}
						index++;
					}

					SXSSFRow nextrow = sheet3.createRow(
							i - (index * (index == 0 ? firstPageMaxCount : pageMaxRowCount) - (index > 0 ? 1 : 0)));
					set = columnDetails.keySet();

					for (String s : set) {

						nextrow.createCell(cellNum).setCellValue(columnDetails.get(s));
						cellNum++;
					}

				}

				Iterator<Sheet> iterator = wb.sheetIterator();
				while (iterator.hasNext()) {
					Sheet sheet = iterator.next();

					for (int i = 0; i < columnNames.size(); i++) {

						sheet.setColumnWidth(i, (int) ((50 + 0.72) * 120));
					}

				}
			} else {

				row = sheet3.createRow(0);

				String[] heads = SQLQUERY_Profile.getEmptyHead(currentQuery);

				int firstPageCellNum = 0;
				for (String s : heads) {
					SXSSFCell cell0 = row.createCell(firstPageCellNum);
					cell0.setCellStyle(headerStyle);
					cell0.setCellValue(s);

					firstPageCellNum++;
				}

				Iterator<Sheet> iterator = wb.sheetIterator();
				while (iterator.hasNext()) {
					Sheet sheet = iterator.next();

					for (int i = 0; i < heads.length; i++) {

						sheet.setColumnWidth(i, (int) ((50 + 0.72) * 120));
					}

				}

			}
			wb.write(fileOut);
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		} finally {

			try {
				if (wb != null) {
					wb.dispose();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				if (fileOut != null) {
					fileOut.flush();
					fileOut.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
