package FTP;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class ETL_SFTP {

	public static void main(String[] args) throws SftpException, IOException {

		String hostName = "172.18.21.206"; // jar檔預設port:22
		String port = "22";
		String username = "tim";
		String password = "tim7146";
		String localhostdir = "\\";
//		File savedir = new File("D:\\testB");


//		List<String> list = new ArrayList<String>();
//		list = listFiles(hostName, Integer.valueOf(port), username, password, localhostdir);
//		for (int i = 0; i < list.size(); i++) {
//			System.out.println(list.get(i));
//		}
		
		String localFilePath = "C:/Users/10404003/Desktop/temp/Sftp_Download/test4.txt";
		
		// 下載
//		SFTP.download(hostName, port, username, password, localFilePath, "/test4.txt");
		
		// 上傳
//		SFTP.upload(hostName, port, username, password, localFilePath, "/test4.txt");
		
		// 刪除
//		ETL_SFTP.delete(hostName, port, username, password, "/test4.txt");
		
		// 列出所有測試
//		List<String> fList = listFiles(hostName, Integer.valueOf(port), username, password, "/");
//		System.out.println("fList size = " + fList.size());
//		for (int i = 0; i < fList.size(); i++) {
//			System.out.println(fList.get(i));
//		}
		
		// 確認檔案是否存在
		boolean isOK = exist(hostName, port, username, password, "/600/UPLOAD/600MASTER.txt");
		System.out.println(isOK);

	}
	

	private static ChannelSftp connect(String host, int port, String username, String password) {
		ChannelSftp sftp = null;
		try {
			JSch jsch = new JSch();
			jsch.getSession(username, host, port);
			Session sshSession = jsch.getSession(username, host, port);
//			System.out.println("Session created");
			sshSession.setPassword(password);
			Properties sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");
			sshSession.setConfig(sshConfig);
			sshSession.connect();
//			System.out.println("Session connected.");
//			System.out.println("Opening Channel.");
			Channel channel = sshSession.openChannel("sftp");
			channel.connect();
			sftp = (ChannelSftp) channel;
//			System.out.println("Connected to " + host);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sftp;
	}
	
	public static List<String> listFiles(String host, int port, String username, String password, String directory) throws SftpException, JSchException {
		System.out.println("listFiles " + directory + " listFiles start");
		ChannelSftp sftp = connect(host, port, username, password);
		List<String> result = new ArrayList<String>();
		List<ChannelSftp.LsEntry> list = sftp.ls(directory);

		for (ChannelSftp.LsEntry entry : list) {
			if (!".".equals(entry.getFilename()) && !"..".equals(entry.getFilename())) {
				result.add(entry.getFilename());
			}
		}
		
		System.out.println("result size " +result.size());
		
		for(String str:result) {
			System.out.println("str " + str);
		}
		
		Session session=sftp.getSession();
		sftp.disconnect();
		session.disconnect();
		
		return result;
	}
	
	public static boolean upload(String hostName, String port, String username, String password, 
			String localFilePath, String remoteFilePath) {

		File f = new File(localFilePath);
		if (!f.exists()) {
			System.out.println("Local file " + remoteFilePath + " not found. Upload failure.");
			
			return false;
		}

		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create local file object
			FileObject localFile = manager.resolveFile(f.getAbsolutePath());

			// Create remote file object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());
			
			if (remoteFile.exists() && remoteFile.isFile()) {
				if (remoteFile.delete()) {
					// Copy local file to sftp server
					remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
				} else {
					System.out.println("File upload failure");
					return false;
				}
			} else {
				// Copy local file to sftp server
				remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
			}

			System.out.println("File upload success");
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			manager.close();
		}
	}

	public static boolean download(String hostName, String port, String username, String password, 
			String localFilePath, String remoteFilePath) {
		
		//TODO FOR TEST
		System.out.println("ETL_SFTP.download....." );// test temp
		System.out.println("hostName = " + hostName);// test temp
		System.out.println("port = " + port);// test temp
		System.out.println("username = " + username);// test temp
		System.out.println("password = " + password);// test temp
		System.out.println("localFilePath = " + localFilePath);// test temp
		System.out.println("remoteFilePath = " + remoteFilePath);// test temp


		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();
			
			String downloadFilePath = localFilePath;
//			String downloadFilePath = localFilePath.substring(0, localFilePath.lastIndexOf("."))
//					+ localFilePath.substring(localFilePath.lastIndexOf("."), localFilePath.length());

			System.out.println("downloadFilePath = " + downloadFilePath);// test temp

			// Create local file object
			FileObject localFile = manager.resolveFile(downloadFilePath);

			// Create remote file object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());
			
			if (remoteFile.exists()) {
				// Copy local file to sftp server
				localFile.copyFrom(remoteFile, Selectors.SELECT_SELF);
	
				System.out.println("File download success");
				
				//成功回傳true
				return true;
			} else {
				
				System.out.println("Remote file " + remoteFilePath + " not found. Download failure.");
				
				// 找不到檔案回傳false
				return false;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
			// 失敗回傳false
			return false;
		} finally {
			manager.close();
		}
	}

	public static boolean delete(String hostName, String port, String username, String password, String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());

			if (remoteFile.exists()) {
				remoteFile.delete();
				System.out.println("Delete remote file success.");
				
				// 成功回傳true
				return true;
			} else {
				System.out.println("Remote file " + remoteFilePath + " not found. Delete failure.");
				
				// 失敗回傳false
				return false;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			// 失敗回傳false
			return false;
		} finally {
			manager.close();
		}
	}
	
	public static boolean exist(String hostName, String port, String username, String password, String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());

			System.out.println("File exist: " + remoteFile.exists());

			return remoteFile.exists();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}
	
	public static boolean moveTo(String hostName, String port, String username, String password, String fromFilePath, String toFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create from File object
			FileObject fromFile = manager.resolveFile (
					createConnectionString(hostName, port, username, password, fromFilePath), createDefaultOptions());
			
			// Create to File object
			FileObject toFile = manager.resolveFile (
					createConnectionString(hostName, port, username, password, toFilePath), createDefaultOptions());

			if (fromFile.exists()) {
				
				toFile.createFile();
				
				fromFile.moveTo(toFile);
				System.out.println("Move file from " + fromFilePath + " to " + toFilePath + " success.");
				
				// 成功回傳true
				return true;
			} else {
				System.out.println("Move file " + fromFilePath + " not found. Move failure.");
				
				// 失敗回傳false
				return false;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			// 失敗回傳false
			return false;
		} finally {
			manager.close();
		}
	}
	
	// 產生SFTP路徑資料夾
	public static boolean createFolder(String hostName, String port, String username, String password, String folderPath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, folderPath), createDefaultOptions());

			if (remoteFile.exists() && remoteFile.isFolder()) {
				System.out.println("路徑資料夾:" + folderPath + "已存在！  不須產生");
				return true;
			} else {
				remoteFile.createFolder();
				if (remoteFile.exists()) {
					System.out.println("路徑資料夾:" + folderPath + "已產生！");
					return true;
				} else {
					System.out.println("路徑資料夾:" + folderPath + "產生失敗！");
					return false;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	private static String createConnectionString(String hostName, String port, String username, String password,
			String remoteFilePath) {
		// result: "sftp://user:123456@domainname.com/resume.pdf
		return "sftp://" + username + ":" + password + "@" + hostName + ":" + port + "/" + remoteFilePath;
	}

	private static FileSystemOptions createDefaultOptions() throws FileSystemException {
		// Create SFTP options
		FileSystemOptions opts = new FileSystemOptions();

		// SSH Key checking
		SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");

		// Root directory set to user home
		SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);

		// Timeout is count by Milliseconds
		SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

		return opts;
	}
	
}
