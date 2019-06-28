package Tool;

import java.io.File;
import java.io.FileFilter;

import Profile.SQLQUERY_Profile;

public class ETL_Tool_Relevance_Upload_FileFilter implements FileFilter {

	@Override
	public boolean accept(File file) {
		boolean isAccept = false;

		if (file == null)
			return false;

		String suffixStr = "";
		String fileName = file.getName();
		String[] orders = SQLQUERY_Profile.getAllOrder();

		if (orders != null) {
			for (String order : orders) {

				if (fileName.contains(order)) {
					isAccept = true;
				}

			}
		}

		if (isAccept) {
			if (fileName.length() > 5) {
				suffixStr = fileName.substring(fileName.length() - 5, fileName.length());
			}
			if (!".xlsx".equals(suffixStr)) {
				isAccept = false;
			}
		}

		return isAccept;
	}

	public static File[] getFiles(File dir) {
		return dir.listFiles(new ETL_Tool_Relevance_Upload_FileFilter());
	}
}
