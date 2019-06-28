package Tool;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ETL_Tool_Encryp {

	private static final char[] hexChar = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
			'f' };

	private static byte[] eccrypt(String info) throws NoSuchAlgorithmException {

		MessageDigest md5 = MessageDigest.getInstance("MD5");
		byte[] srcBytes = info.getBytes();

		md5.update(srcBytes);

		byte[] resultBytes = md5.digest();
		return resultBytes;
	}

	public static String encode(String msg) throws NoSuchAlgorithmException {
		String result = null;
		byte[] resultBytes = ETL_Tool_Encryp.eccrypt(msg);
		result = toHexString(resultBytes);
		if (result.length() > 20) {
			result = result.substring(8, 18);
		}

		return result;
	}

	private static String toHexString(byte[] b) {
		StringBuilder sb = new StringBuilder(b.length * 2);
		for (int i = 0; i < b.length; i++) {
			sb.append(hexChar[(b[i] & 0xf0) >>> 4]);
			sb.append(hexChar[b[i] & 0x0f]);
		}
		return sb.toString();
	}
	public static void main(String[] args) throws NoSuchAlgorithmException {
		System.out.println(encode("AML_018_20181112001.zip"));
		System.out.println(encode("AML_600_20181112001.zip"));
		System.out.println(encode("AML_910_20181109001.zip"));
		System.out.println(encode("AML_928_20181108001.zip"));
		System.out.println(encode("AML_951_20181108001.zip"));
		System.out.println(encode("AML_018_20181214002.zip"));
	}
}