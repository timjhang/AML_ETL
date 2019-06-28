package Tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import java.util.Vector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import Bean.ETL_Bean_SFTP_Data;
import de.idyl.winzipaes.AesZipFileEncrypter;
import de.idyl.winzipaes.impl.AESEncrypter;
import de.idyl.winzipaes.impl.AESEncrypterBC;

public class ETL_Tool_RelevanceSql_To_Excels_SFTP {
	private Properties prop = null;

	public ETL_Tool_RelevanceSql_To_Excels_SFTP(String path) {
		try {
			prop = new Properties();
			prop.load(new FileInputStream(path));
		} catch (Exception e) {
			System.out.println(getStackTrace(e));
		}
	}

	public ETL_Bean_SFTP_Data getData(String centralNo) {

		ETL_Bean_SFTP_Data sftpData = null;
		String sftpUser = null, pwd = null, host = null, directory = null;
		int port = 0;
		try {
			sftpData = new ETL_Bean_SFTP_Data();

			// get value
			host = prop.getProperty("sftp.host");
			directory = prop.getProperty("sftp.directory");
			port = Integer.parseInt(prop.getProperty("sftp.port"));
			sftpUser = prop.getProperty(centralNo + ".sftp.user");
			pwd = prop.getProperty(centralNo + ".sftp.pass");
			pwd = ETL_Tool_DES.decrypt(pwd);

			// set value
			sftpData.setSftpUser(sftpUser);
			sftpData.setPwd(pwd);
			sftpData.setHost(host);
			sftpData.setPort(port);
			sftpData.setDirectory(directory);
		} catch (Exception e) {
			System.out.println(getStackTrace(e));
		}
		return sftpData;
	}

	public ChannelSftp connect(String centralNo, ETL_Bean_SFTP_Data data) {
		System.out.println("SFTP connect start.");
		Channel channel = null;
		ChannelSftp sftp = null;
		JSch jsch = null;
		Session sshSession = null;
		Properties sshConfig = null;
		String sftpUser = null, pwd = null, host = null;
		int port = 0;
		try {
			host = data.getHost();
			port = data.getPort();
			sftpUser = data.getSftpUser();
			pwd = data.getPwd();

			jsch = new JSch();

			sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");
			
			sshSession = jsch.getSession(sftpUser, host, port);
			sshSession.setPassword(pwd);
			sshSession.setConfig(sshConfig);
			sshSession.setTimeout(600000);
			System.out.println("SFTP session created.");

			sshSession.connect();
			System.out.println("SFTP session connected.");

			channel = sshSession.openChannel("sftp");
			System.out.println("Opening SFTP channel.");

			channel.connect();
			sftp = (ChannelSftp) channel;
			System.out.println("Connected to " + host + ".");
		} catch (Exception e) {
			System.out.println(getStackTrace(e));
		}
		System.out.println("Return SFTP channel.");
		return sftp;
	}

	public boolean upload(String directory, File uploadFile, ChannelSftp sftp) {
		boolean isSuccess = false;
		try {
			sftp.cd(directory);
			sftp.put(new FileInputStream(uploadFile), uploadFile.getName());
			isSuccess = true;
		} catch (Exception e) {
			System.out.println(getStackTrace(e));
			return false;
		}
		return isSuccess;
	}

	@SuppressWarnings("unchecked")
	public boolean makeDir(String path, String directory, ChannelSftp sftp) throws SftpException {

		boolean isExist = false;
		Vector<LsEntry> vector = null;

		try {
			vector = sftp.ls(path);
			for (int i = 0; i < vector.size(); i++) {
				LsEntry entry = vector.get(i);

				if (entry.getFilename().equalsIgnoreCase(directory)) {
					isExist = true;
				}
			}
			if (!isExist) {
				sftp.cd(path);
				sftp.mkdir(directory);
			}
		} catch (Exception e) {
			System.out.println(getStackTrace(e));
		}
		return isExist;
	}

	@SuppressWarnings("unchecked")
	public static boolean isFileExist(String path, String fileName, ChannelSftp sftp) {

		boolean isExist = false;
		Vector<LsEntry> vector = null;
		try {
			vector = sftp.ls(path);
			isExist = false;
			for (int i = 0; i < vector.size(); i++) {
				LsEntry entry = vector.get(i);

				if (entry.getFilename().equalsIgnoreCase(fileName)) {
					isExist = true;
				}
			}
		} catch (Exception e) {
			System.out.println(getStackTrace(e));
		}
		return isExist;
	}

	public File zipFilesByWinzipaes(String savePath, String currdir) {
		System.out.println("zipFilesByWinzipaes start.");

		File currFile = null;
		File destFile = null;

		String zipName = null;

		AesZipFileEncrypter ze = null;
		FileInputStream fis = null;
		AESEncrypter aesEncrypter = null;

		try {
			currFile = new File(currdir);
			zipName = currFile.getName() + ".zip";
			destFile = new File(savePath, zipName);

			aesEncrypter = new AESEncrypterBC();
			ze = new AesZipFileEncrypter(destFile, aesEncrypter);

			String password = ETL_Tool_Encryp.encode(zipName);

			File[] files = ETL_Tool_Relevance_Upload_FileFilter.getFiles(currFile);

			for (File srcFile : files) {
				try {
					fis = new FileInputStream(srcFile);
					ze.setEncoding("UTF-8");
					ze.add(srcFile.getName(), fis, password);
				} catch (Exception e) {
					System.out.println("執行壓縮文字檔時失敗:" + getStackTrace(e));
				} finally {
					if (fis != null) {
						try {
							fis.close();
						} catch (IOException e) {
							System.out.println("執行壓縮文字檔時，關閉資源失敗:" + getStackTrace(e));
						}
					}
				}
			}
		} catch (Exception e) {
			System.out.println(getStackTrace(e));
		} finally {
			if (ze != null) {
				try {
					ze.close();
				} catch (IOException e) {
					System.out.println("執行壓縮文字檔時，關閉資源失敗:" + getStackTrace(e));
				}
			}
		}

		System.out.println("zipFilesByWinzipaes end");
		return destFile;
	}

	public static String getStackTrace(final Throwable throwable) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw, true);
		throwable.printStackTrace(pw);
		return sw.getBuffer().toString();
	}

}

