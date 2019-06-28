package Tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
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

import com.jcraft.jsch.ChannelSftp;

import Bean.ETL_Bean_SFTP_Data;
import Bean.ETL_DETAIL_LOG;
import Bean.ETL_FILE_LOG;
import DB.ConnectionHelper;
import DB.JDBC;
import Profile.SQLQUERY_Profile;
import Tool.ETL_Tool_RelevanceSql_To_Excels_SFTP;
import Tool.ETL_Tool_Relevance_Delete_FileFilter;
import Tool.ETL_Tool_Relevance_Upload_FileFilter;

public class ETL_Tool_RelevanceSql_To_Excels {

	private final int pageMaxRowCount = 65534;
	private final String propertiesPath = "D:\\DataCheck\\sftp.properties";

	private String batch_no;
	private String record_date = null;
	private String currentQuery = null;
	private String savePath = null;
	private String currDir = null;
	private String yearDir = null;
	private String monthDir = null;
	private java.util.Date recordUtilDate = null;
	private java.sql.Date recordSqlDate = null;

	private SimpleDateFormat sdf = null;
	private ArrayList<String> columnNames = null;
	private static Map<String, Integer> dataMap = null;

	public ETL_Tool_RelevanceSql_To_Excels() {
	}

	public ETL_Tool_RelevanceSql_To_Excels(String batch_no, String record_date) throws ParseException {

		this.batch_no = batch_no;
		this.record_date = record_date;

		sdf = new SimpleDateFormat("yyyy-MM-dd");
		this.recordUtilDate = sdf.parse(record_date);
		this.recordSqlDate = new java.sql.Date(recordUtilDate.getTime());

		sdf = new SimpleDateFormat("MM");
		this.monthDir = sdf.format(recordUtilDate);

		sdf = new SimpleDateFormat("yyyy");
		this.yearDir = sdf.format(recordUtilDate);
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException, ParseException {

		long time1, time2;

		time1 = System.currentTimeMillis();

		// boolean success = new ETL_Tool_RelevanceSql_To_Excels("R0000011",
		// "2018-10-22").execute("018");
		boolean success = new ETL_Tool_RelevanceSql_To_Excels(args[0], args[1]).execute(args[2]);

		time2 = System.currentTimeMillis();

		System.out.println("執行結果" + (success ? "成功" : "失敗") + "/花了：" + (time2 - time1) + "豪秒");
	}

	public boolean execute(String centralNos)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String[] centralNoarr = null, querys = null;

		JDBC jdbc = null;
		File zipFile = null;
		ChannelSftp channelSftp = null;

		ETL_FILE_LOG fileLog = null;
		ETL_DETAIL_LOG detailLog = null;

		boolean isSuccess = false;

		ETL_Bean_SFTP_Data data = null;
		try {
			centralNoarr = centralNos.split(",");
			querys = SQLQUERY_Profile.getQuerys();

			jdbc = new JDBC();

			for (String centralNo : centralNoarr) {
				this.savePath = "D:" + File.separator + "DataCheck" + File.separator + centralNo;

				System.out.println("\n執行單位:" + centralNo);

				detailLog = new ETL_DETAIL_LOG();
				detailLog.setBatch_no(batch_no);
				detailLog.setCentral_no(centralNo);
				detailLog.setRecord_date(recordSqlDate);
				detailLog.setExe_status("S");
				detailLog.setExe_result("");
				detailLog.setExe_result_description("執行中");

				jdbc.insertLog(detailLog);

				try {

					dataMap = new HashMap<String, Integer>();

					for (String query : querys) {

						fileLog = new ETL_FILE_LOG();
						fileLog.setBatch_no(batch_no);
						fileLog.setCentral_no(centralNo);
						fileLog.setRecord_date(recordSqlDate);
						fileLog.setFile_name(SQLQUERY_Profile.getFileName(query));
						fileLog.setExe_result("");
						fileLog.setExe_result_description("執行中");
						fileLog.setSrc_file(SQLQUERY_Profile.getSrcFile(query));
						jdbc.insertLog(fileLog);

						try {
							ResultSet rs = getResultsetFromSql(centralNo, query);
							generateExcel(centralNo, processResultSet(rs));

							fileLog.setExe_result("Y");
							fileLog.setExe_result_description("無錯誤資料");
							jdbc.updateLog(fileLog);
						} catch (Exception e) {

							fileLog.setExe_result("S");
							fileLog.setExe_result_description(getDescription(e));
							jdbc.updateLog(fileLog);
						}

					}

					detailLog.setExe_status("E");
					detailLog.setExe_result("Y");
					detailLog.setExe_result_description("無錯誤資料");

					jdbc.updateLog(detailLog);
				} catch (Exception e) {
					e.printStackTrace();
					detailLog.setExe_status("E");
					detailLog.setExe_result("S");
					detailLog.setExe_result_description(getDescription(e));
					jdbc.updateLog(detailLog);
				}

				generateDataCheckExcel(centralNo);

				System.out.println(centralNo + "關連檔產生完成");

				ETL_Tool_RelevanceSql_To_Excels_SFTP sftp = new ETL_Tool_RelevanceSql_To_Excels_SFTP(propertiesPath);

				System.out.println("進行壓縮關聯檔作業..");
				zipFile = sftp.zipFilesByWinzipaes(savePath, currDir);

				if (zipFile != null) {
					System.out.printf("壓縮成功，檔名:%s\n", zipFile.getName());
					data = sftp.getData(centralNo);
					channelSftp = sftp.connect(centralNo, data);

					String currDir = data.getDirectory();

					sftp.makeDir(currDir, yearDir, channelSftp);
					currDir = currDir + "/" + yearDir;

					sftp.makeDir(currDir, monthDir, channelSftp);
					currDir = currDir + "/" + monthDir;

					sftp.upload(currDir, zipFile, channelSftp);
					System.out.printf("上傳成功，放置SFTP SERVER路徑:%s\n", currDir);

					channelSftp.disconnect();
					System.out.println("SFTP channel disconnect.");

					System.out.printf("檢查是否需要進行刪除動作，移除未保留資料:");
					File[] files = ETL_Tool_Relevance_Delete_FileFilter.getFiles(savePath, yearDir, monthDir);
					int delNum = files.length;

					System.out.println(delNum == 0 ? "否" : "是，需刪除" + delNum + "筆");

					for (int i = 0; i < delNum; i++) {
						File file = files[i];
						Path dir = Paths.get(file.getAbsolutePath());

						System.out.println(
								(i + 1) + ".刪除" + (file.isDirectory() ? "資料夾:" : "壓縮檔:") + dir.toAbsolutePath());

						try {
							Files.deleteIfExists(dir);
						} catch (DirectoryNotEmptyException e) {
							Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
								@Override
								public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
										throws IOException {
									Files.delete(file);
									return FileVisitResult.CONTINUE;
								}

								@Override
								public FileVisitResult postVisitDirectory(Path dir, IOException exc)
										throws IOException {
									Files.delete(dir);
									return super.postVisitDirectory(dir, exc);
								}
							});
						} catch (Exception e) {
							System.out.println("刪除失敗，錯誤訊息:" + getDescription(e));
						}
					}
				} else {
					System.out.println("壓縮關聯檔作業異常");
				}
			}
			isSuccess = true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return isSuccess;
	}

	// 傳回給定的sql查詢的ResultSet物件
	public ResultSet getResultsetFromSql(String centralNo, String sql)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// Connection conn = getDBConnection();
		this.currentQuery = sql;
		Connection conn = ConnectionHelper.getDB2ConnGAML(centralNo);
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

	// 將查詢回來的ResultSet物件，處理成Map型態
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

	// 產出ETL傳檔資料關聯檢核總表
	public void generateDataCheckExcel(String centralNo) {
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

			String fileName = centralNo + "_ETL傳檔資料關聯檢核總表_" + record_date.replaceAll("-", "");

			File file = new File(savePath + File.separator + centralNo + "_ETL關聯檢查_" + record_date.replaceAll("-", "")
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

	// 產出關聯檔
	public void generateExcel(String centralNo, Map<String, LinkedHashMap<String, String>> resultMap) {
		FileOutputStream fileOut = null;
		SXSSFWorkbook wb = null;
		String key = null;

		try {
			StringBuilder fileName = new StringBuilder(centralNo);
			fileName = SQLQUERY_Profile.getFileNameDetail(fileName, currentQuery);

			key = fileName.toString().replaceFirst(centralNo, "").replaceAll("_", "").replaceAll("[0-9]", "");

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
				currDir = savePath + File.separator + centralNo + "_ETL關聯檢查_" + record_date.replaceAll("-", "");
				File file = new File(currDir + File.separator + fileName + ".xlsx");
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

	private String getDescription(Exception e) {
		String description = null;
		if (e != null) {
			description = e.toString();
			description = description.length() > 400 ? description.substring(0, 400) : description;
		}
		return description;
	}

}
