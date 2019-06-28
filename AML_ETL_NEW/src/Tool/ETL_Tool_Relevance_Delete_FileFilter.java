package Tool;

import java.io.File;
import java.io.FileFilter;

public class ETL_Tool_Relevance_Delete_FileFilter implements FileFilter {

	private String yearDir = null;
	private String monthDir = null;

	public ETL_Tool_Relevance_Delete_FileFilter(String yearDir, String monthDir) {
		this.yearDir = yearDir;
		this.monthDir = monthDir;
	}

	@Override
	public boolean accept(File file) {
		boolean isAccept = false;
		boolean isZip = false;
		boolean isDirectory = false;

		if (file == null)
			return false;

		String fileName = file.getName();

		if (fileName.length() >= 12) {

			String suffixStr = null;
			String dateStr = null;
			String currDateStr = yearDir + monthDir + "01";

			suffixStr = fileName.substring(fileName.length() - 3, fileName.length());
			if ("zip".equalsIgnoreCase(suffixStr))
				isZip = true;

			if (file.isDirectory())
				isDirectory = true;

			if (isDirectory) {
				dateStr = fileName.substring(fileName.length() - 8, fileName.length());
			} else if (isZip) {
				dateStr = fileName.substring(fileName.length() - 12, fileName.length() - 4);
			}

			if (isDirectory || isZip) {
				if (ETL_Tool_FormatCheck.checkDate(dateStr, "yyyyMMdd")) {

					int currDateInt = Integer.valueOf(currDateStr);
					int dateInt = Integer.valueOf(dateStr);
					if (dateInt < currDateInt)
						isAccept = true;
				}
			}

		}
		return isAccept;
	}

	public static File[] getFiles(String path, String yearDir, String monthDir) {

		return new File(path).listFiles(new ETL_Tool_Relevance_Delete_FileFilter(yearDir, monthDir));
	}
}