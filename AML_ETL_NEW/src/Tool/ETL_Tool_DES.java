package Tool;

import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

public class ETL_Tool_DES {

	private static final String KEY = "6tfcnhy6";

	private static byte[] encryptBytes(String content) {
		try {
			SecureRandom random = new SecureRandom();
			DESKeySpec desKey = new DESKeySpec(KEY.getBytes());
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
			SecretKey securekey = keyFactory.generateSecret(desKey);
			Cipher cipher = Cipher.getInstance("DES");
			cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
			byte[] result = cipher.doFinal(content.getBytes());
			return result;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String decrypt(String hexContent) {
		try {
			byte[] content = hexStr2ByteArr(hexContent);
			SecureRandom random = new SecureRandom();
			DESKeySpec desKey = new DESKeySpec(KEY.getBytes());
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
			SecretKey securekey = keyFactory.generateSecret(desKey);
			Cipher cipher = Cipher.getInstance("DES");
			cipher.init(Cipher.DECRYPT_MODE, securekey, random);
			byte[] result = cipher.doFinal(content);
			return new String(result);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	private static String byteArr2HexStr(byte[] arrB) throws Exception {
		int iLen = arrB.length;
		// 每個byte用兩個字符才能表示，所以字符串的長度是數組長度的兩倍
		StringBuffer sb = new StringBuffer(iLen * 2);
		for (int i = 0; i < iLen; i++) {
			int intTmp = arrB[i];
			// 把負數轉換為正數
			while (intTmp < 0) {
				intTmp = intTmp + 256;
			}
			// 小於0F的數需要在前補0
			if (intTmp < 16) {
				sb.append("0");
			}
			sb.append(Integer.toString(intTmp, 16));
		}
		return sb.toString();
	}

	private static byte[] hexStr2ByteArr(String strIn) throws Exception {
		byte[] arrB = strIn.getBytes();
		int iLen = arrB.length;

		// 兩個字符表示一個字節，所以字節數組長度是字符串長度除以2
		byte[] arrOut = new byte[iLen / 2];
		for (int i = 0; i < iLen; i = i + 2) {
			String strTmp = new String(arrB, i, 2);
			arrOut[i / 2] = (byte) Integer.parseInt(strTmp, 16);
		}
		return arrOut;
	}
	
	private static String encrypt(String input) throws Exception {
		byte[] result = encryptBytes(input);
		
		return byteArr2HexStr(result);
	}

	public static void main(String[] args) {
//		byte[] result = encryptBytes("");
		try {
//			System.out.println(byteArr2HexStr(result));
//			System.out.println(decrypt(""));
			
//			System.out.println(encrypt("jdbc:db2://172.18.6.133:50000/GAMLDB:currentschema=SRC;currentFunctionPath=SRC;"));
//			System.out.println(encrypt("GAMLETL"));
//			System.out.println(encrypt("1qaz@WSX"));
//			System.out.println(encrypt("50000"));
//			System.out.println(encrypt("etlpassw0rd"));
//			System.out.println(encrypt(":currentschema=SRC;currentFunctionPath=SRC;"));
			
//			System.out.println(decrypt("decbb834578d510a250db3230e803611"));
//			System.out.println(decrypt("de6964a598384312250db3230e803611"));
			
//			System.out.println(encrypt("jdbc:db2://172.18.6.152:50000/ETLDB002:currentschema=ADMINISTRATOR;currentFunctionPath=ADMINISTRATOR;"));
//			System.out.println(encrypt("C:/ETL/L_Script/run/"));
			
//			System.out.println(decrypt("8ba6ba4bf4a45186b96fcc12bd01445545021e9d1fe4431eae94124ee542db4e160dd19b9ddd3c9a"));
//			System.out.println(decrypt("6366786fdfbad193"));
			
//			System.out.println(encrypt("5325Etlpassw0rd"));
			
			System.out.println(encrypt("ETLUSR"));
			System.out.println(encrypt("1qazXSW@"));
			System.out.println(decrypt("63533c58288e861e"));
			System.out.println(decrypt("75389c596a389dcfc53af1a4a004b34f"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
