package Profile;

import java.util.HashMap;
import java.util.Map;

public class SQLQUERY_Profile {

	// Transaction 資料都串得到 Account
	// XXX_交易明細檔帳號在帳戶檔無資料_yyyymmdd
	private static String SQLQUERY1 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(A.ACCOUNT_KEY,9,30) AS 帳號, A.TRANSACTION_DATE AS 交易日期, A.EXECUTION_BRANCH_CODE AS 交易執行分行, A.TRANSACTION_TYPE AS 交易類別 FROM SRC.TRANSACTION A WHERE NOT EXISTS (SELECT 1 FROM SRC.ACCOUNT B WHERE A.ACCOUNT_KEY = B.ACCOUNT_KEY AND B.RECORD_DATE = ?) AND A.DATA_SOURCE_FILE_NAME IN ('SELF') AND A.ETL_CREATE_DATE = ? ORDER BY A.FILE_TYPE";

	// Account 資料透過 Party Account Rel 都串得到 Party
	// XXX_帳戶檔客戶在顧客主檔無資料_yyyymmdd
	private static String SQLQUERY2 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(B.PARTY_KEY,9,11) AS 客戶統編, SUBSTRING(A.ACCOUNT_KEY,9,30) AS 帳號, A.ACCOUNT_TYPE_CODE AS 帳戶類別, A.STATUS_CODE AS 帳戶狀態 FROM SRC.ACCOUNT A LEFT JOIN SRC.PARTY_ACCOUNT_REL B  ON A.ACCOUNT_KEY = B.ACCOUNT_KEY  AND A.RECORD_DATE = B.RECORD_DATE WHERE NOT EXISTS (SELECT 1 FROM SRC.PARTY C WHERE C.PARTY_KEY = B.PARTY_KEY AND C.RECORD_DATE = B.RECORD_DATE) AND A.DATA_SOURCE_FILE_NAME IN ('SELF') AND A.RECORD_DATE = ? ORDER BY A.FILE_TYPE";

	// Loan Detail 資料都串得到 Loan Master
	// XXX_批覆書額度主檔在顧客主檔無資料_yyyymmdd
	private static String SQLQUERY3 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(B.PARTY_KEY,9,11) AS 客戶統編, SUBSTRING(A.LOAN_DETAIL_KEY,9,20) AS 額度編號, A.APPROVAL_DATE AS 額度核准日, SUBSTRING(A.LOAN_MASTER_KEY,9,20) AS 批覆書編號, A.RECORD_CREATE_DATE AS 批覆書申請日 FROM SRC.LOAN_DETAIL A LEFT JOIN SRC.LOAN_MASTER B  ON A.LOAN_MASTER_KEY = B.LOAN_MASTER_KEY  AND A.RECORD_DATE = B.RECORD_DATE WHERE NOT EXISTS (SELECT 1 FROM SRC.PARTY C WHERE C.PARTY_KEY = B.PARTY_KEY AND C.RECORD_DATE = B.RECORD_DATE) AND A.RECORD_DATE = ? ORDER BY A.FILE_TYPE";

	// Loan 資料都串得到 Loan Detail
	// XXX_放款主檔在額度檔無資料_yyyymmdd
	private static String SQLQUERY4 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號,  SUBSTRING(A.LOAN_KEY,9,30) AS 放款帳號, SUBSTRING(A.LOAN_DETAIL_KEY,9,20) AS 額度編號, A.LOAN_DATE AS 初貸日, A.LOAN_STATUS_CODE AS 放款狀態 FROM SRC.LOAN A WHERE NOT EXISTS (SELECT 1 FROM SRC.LOAN_DETAIL B WHERE A.LOAN_DETAIL_KEY = B.LOAN_DETAIL_KEY AND A.RECORD_DATE = B.RECORD_DATE) AND A.RECORD_DATE = ? ORDER BY A.FILE_TYPE";

	// Loan 資料都串得到 Account
	// XXX_放款主檔帳號在帳戶檔無資料_yyyymmdd
	private static String SQLQUERY5 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(A.LOAN_KEY,9,30) AS 放款帳號, SUBSTRING(A.LOAN_DETAIL_KEY,9,20) AS 額度編號, A.LOAN_DATE AS 初貸日, A.LOAN_STATUS_CODE AS 放款狀態 FROM SRC.LOAN A WHERE NOT EXISTS (SELECT 1 FROM SRC.ACCOUNT B WHERE A.LOAN_KEY = B.ACCOUNT_KEY AND A.RECORD_DATE = B.RECORD_DATE) AND A.RECORD_DATE = ? ORDER BY A.FILE_TYPE ";

	// 擔保品 都串得到 Loan Detail
	// XXX_擔保品檔在額度檔無資料_yyyymmdd
	private static String SQLQUERY6 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, SUBSTRING(A.LOAN_DETAIL_KEY,1,7) AS 本會代號,  SUBSTRING(A.COLLATERAL_KEY,19,20) AS 擔保品編號, SUBSTRING(A.LOAN_DETAIL_KEY,9,20) AS 額度編號 FROM SRC.LOAN_COLLATERAL A WHERE NOT EXISTS (SELECT 1 FROM SRC.LOAN_DETAIL B WHERE A.LOAN_DETAIL_KEY = B.LOAN_DETAIL_KEY AND A.RECORD_DATE = B.RECORD_DATE) AND A.RECORD_DATE = ? ORDER BY A.FILE_TYPE";

	// 保證人 都串得到 Loan Detail
	// XXX_保證人檔在額度檔無資料_yyyymmdd
	private static String SQLQUERY7 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, SUBSTRING(A.LOAN_DETAIL_KEY,1,7) AS 本會代號, SUBSTRING(A.GUARANTOR_KEY,25,11) AS 保證人統編, SUBSTRING(A.LOAN_DETAIL_KEY,9,20) AS 額度編號 FROM SRC.LOAN_GUARANTOR A WHERE NOT EXISTS (SELECT 1 FROM SRC.LOAN_DETAIL B WHERE A.LOAN_DETAIL_KEY = B.LOAN_DETAIL_KEY  AND A.RECORD_DATE = B.RECORD_DATE) AND A.RECORD_DATE = ? ORDER BY A.FILE_TYPE";

	// FCX 資料都串得到 Party
	// XXX_外幣現鈔買賣檔在顧客主檔無資料_yyyymmdd
	private static String SQLQUERY8 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, A.TRANSFER_ID AS 交易編號, SUBSTRING(A.PARTY_KEY,9,11) AS 客戶統編, TRANSFER_TIME AS 實際交易時間, A.TRANSACTION_TYPE AS 交易類別 FROM SRC.TRANSFER A WHERE NOT EXISTS (SELECT 1 FROM SRC.PARTY B WHERE A.PARTY_KEY = B.PARTY_KEY AND B.RECORD_DATE = ?) AND A.DATA_SOURCE_FILE_NAME IN ('FCX') AND A.ETL_CREATE_DATE = ? ORDER BY A.FILE_TYPE";

	// Transfer 資料都串得到 Party
	// XXX_境外交易檔在顧客主檔無資料_yyyymmdd
	private static String SQLQUERY9 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, A.TRANSFER_ID AS 匯款編號, SUBSTRING(A.PARTY_KEY,9,11) AS 客戶統編, TRANSFER_TIME AS 實際匯款時間, A.TRANSACTION_TYPE AS 交易類別 FROM SRC.TRANSFER A WHERE NOT EXISTS (SELECT 1 FROM SRC.PARTY B WHERE A.PARTY_KEY = B.PARTY_KEY AND B.RECORD_DATE = ?) AND  A.DATA_SOURCE_FILE_NAME IN ('SELF')  AND A.ETL_CREATE_DATE = ? ORDER BY A.FILE_TYPE";

	// Party 非本行客戶資料都串得到 Party_Party_Rel
	// XXX_顧客主檔非本行客戶在關係人檔無資料_yyyymmdd
	private static String SQLQUERY10 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, A.BRANCH_CODE AS 歸屬本分會代號, SUBSTRING(A.PARTY_KEY,9,11) AS 客戶統編, A.MY_CUSTOMER_FLAG AS 是否本行客戶 FROM SRC.PARTY A WHERE  A.DATA_SOURCE_FILE_NAME IN ('SELF') AND A.MY_CUSTOMER_FLAG = 'N' AND A.RECORD_DATE = ? AND ( NOT EXISTS (SELECT 1 FROM SRC.PARTY_PARTY_REL B WHERE A.PARTY_KEY = B.PARTY_KEY_1 AND A.RECORD_DATE = B.RECORD_DATE) OR NOT EXISTS (SELECT 1 FROM SRC.PARTY_PARTY_REL B WHERE A.PARTY_KEY = B.PARTY_KEY_2 AND A.RECORD_DATE = B.RECORD_DATE)) ORDER BY A.BRANCH_CODE";

	// Party_Party_Rel資料都串得到 Party
	// XXX_關係人檔客戶在顧客主檔無資料_yyyymmdd
	private static String SQLQUERY11 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(A.PARTY_KEY_1,9,11) AS 顧客1統編,  REPLACE(B.PARTY_LAST_NAME_1,SUBSTRING(B.PARTY_LAST_NAME_1,2,2),'OO') AS 顧客1名稱, SUBSTRING(A.PARTY_KEY_2,9,11) AS 顧客2統編,  REPLACE(C.PARTY_LAST_NAME_1,SUBSTRING(C.PARTY_LAST_NAME_1,2,2),'OO') AS 顧客2名稱, A.RELATION_TYPE_CODE AS 顧客關係種類代碼, D.NAME AS 代碼名稱 FROM SRC.PARTY_PARTY_REL A LEFT JOIN SRC.PARTY B ON A.PARTY_KEY_1 = B.PARTY_KEY AND A.RECORD_DATE = B.RECORD_DATE LEFT JOIN SRC.PARTY C ON A.PARTY_KEY_2 = C.PARTY_KEY AND A.RECORD_DATE = C.RECORD_DATE LEFT JOIN GAMLDB.SRC.CODEPOOL D ON A.RELATION_TYPE_CODE = D.CODE AND D.TABLESPACENAME = 'PARTY_PARTY_REL_RELATION_TYPE_CODE' WHERE A.DATA_SOURCE_FILE_NAME IN ('SELF') AND A.REVERSE_FLAG = 'N' AND A.RELATION_TYPE_CODE <> 'S' AND A.RECORD_DATE = ? AND (B.PARTY_LAST_NAME_1 IS NULL OR C.PARTY_LAST_NAME_1 IS NULL) ORDER BY A.DOMAIN_ID, SUBSTRING(A.PARTY_KEY_1,9,11), SUBSTRING(A.PARTY_KEY_2,9,11)";

	// 有Transaction 資料但Party檔非本行客戶
	// XXX_交易明細檔帳戶在顧客主檔非本行客戶資料_yyyymmdd
	private static String SQLQUERY12 = "SELECT DISTINCT A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(A.ACCOUNT_KEY,9,30) AS 帳號, C.BRANCH_CODE AS 歸屬本分會代號, SUBSTRING(B.PARTY_KEY,9,11) AS 客戶統編, C.MY_CUSTOMER_FLAG AS 是否本行客戶 FROM SRC.TRANSACTION A LEFT JOIN SRC.PARTY_ACCOUNT_REL B ON A.ACCOUNT_KEY = B.ACCOUNT_KEY AND B.RECORD_DATE = ? LEFT JOIN SRC.PARTY C ON B.PARTY_KEY = C.PARTY_KEY AND B.RECORD_DATE = C.RECORD_DATE WHERE A.DATA_SOURCE_FILE_NAME IN ('SELF') AND C.MY_CUSTOMER_FLAG = 'N' ORDER BY C.BRANCH_CODE, A.FILE_TYPE";

	// 有Loan_Detail資料但Party檔非本行客戶
	// XXX_批覆書額度主檔帳戶在顧客主檔非本行客戶資料_yyyymmdd
	private static String SQLQUERY13 = "SELECT DISTINCT A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(A.LOAN_DETAIL_KEY,9,20) AS 額度編號, A.APPROVAL_DATE AS 額度核准日, SUBSTRING(A.LOAN_MASTER_KEY,9,20) AS 批覆書編號, A.RECORD_CREATE_DATE AS 批覆書申請日, C.BRANCH_CODE AS 歸屬本分會代號, SUBSTRING(B.PARTY_KEY,9,11) AS 客戶統編, C.MY_CUSTOMER_FLAG AS 是否本行客戶 FROM SRC.LOAN_DETAIL A LEFT JOIN SRC.LOAN_MASTER B  ON A.LOAN_MASTER_KEY = B.LOAN_MASTER_KEY AND A.RECORD_DATE = B.RECORD_DATE LEFT JOIN SRC.PARTY C ON B.PARTY_KEY = C.PARTY_KEY AND B.RECORD_DATE = C.RECORD_DATE WHERE C.MY_CUSTOMER_FLAG = 'N' AND C.RECORD_DATE = ? ORDER BY C.BRANCH_CODE, A.FILE_TYPE, SUBSTRING(B.PARTY_KEY,9,11)";

	// Party_Party_Rel資料都串得到 Party
	// XXX_關係人檔客戶在顧客主檔無實質受益人資料_yyyymmdd
	private static String SQLQUERY14 = "SELECT DISTINCT A.ETL_CREATE_DATE AS 檔案日期, A.FILE_TYPE AS 業務別, A.DOMAIN_ID AS 本會代號, SUBSTRING(A.PARTY_KEY_1,9,11) AS 顧客1統編,  REPLACE(B.PARTY_LAST_NAME_1,SUBSTRING(B.PARTY_LAST_NAME_1,2,2),'OO') AS 顧客1名稱, SUBSTRING(A.PARTY_KEY_2,9,11) AS 顧客2統編,  REPLACE(C.PARTY_LAST_NAME_1,SUBSTRING(C.PARTY_LAST_NAME_1,2,2),'OO') AS 顧客2名稱, A.RELATION_TYPE_CODE AS 顧客關係種類代碼, D.NAME AS 代碼名稱 FROM SRC.PARTY_PARTY_REL A LEFT JOIN SRC.PARTY B ON A.PARTY_KEY_1 = B.PARTY_KEY AND A.RECORD_DATE = B.RECORD_DATE LEFT JOIN SRC.PARTY C ON A.PARTY_KEY_2 = C.PARTY_KEY AND A.RECORD_DATE = C.RECORD_DATE LEFT JOIN GAMLDB.SRC.CODEPOOL D ON A.RELATION_TYPE_CODE = D.CODE AND D.TABLESPACENAME = 'PARTY_PARTY_REL_RELATION_TYPE_CODE' WHERE A.DATA_SOURCE_FILE_NAME IN ('SELF') AND A.REVERSE_FLAG = 'N' AND A.RELATION_TYPE_CODE = 'B' AND A.RECORD_DATE = ? AND (B.PARTY_LAST_NAME_1 IS NULL OR C.PARTY_LAST_NAME_1 IS NULL) ORDER BY A.DOMAIN_ID, SUBSTRING(A.PARTY_KEY_1,9,11), SUBSTRING(A.PARTY_KEY_2,9,11)";

	public static String[] getQuerys() {
		String[] querys = { SQLQUERY1, SQLQUERY2, SQLQUERY3, SQLQUERY4, SQLQUERY5, SQLQUERY6, SQLQUERY7, SQLQUERY8,
				SQLQUERY9, SQLQUERY10, SQLQUERY11, SQLQUERY12, SQLQUERY13, SQLQUERY14 };
		return querys;
	}

	public static String[] getSortOrder() {
		String[] sortOrder = { "交易明細檔帳號在帳戶檔無資料", "帳戶檔客戶在顧客主檔無資料", "批覆書額度主檔在顧客主檔無資料", "放款主檔在額度檔無資料", "放款主檔帳號在帳戶檔無資料",
				"擔保品檔在額度檔無資料", "保證人檔在額度檔無資料", "外幣現鈔買賣檔在顧客主檔無資料", "境外交易檔在顧客主檔無資料", "顧客主檔非本行客戶在關係人檔無資料", "關係人檔客戶在顧客主檔無資料",
				"交易明細檔帳戶在顧客主檔非本行客戶資料", "批覆書額度主檔帳戶在顧客主檔非本行客戶資料", "關係人檔客戶在顧客主檔無實質受益人資料" };
		return sortOrder;
	}

	public static String getFileName(String sql) {
		Map<String, String> map = new HashMap<String, String>();
		map.put(SQLQUERY1, "ACCOUNT");
		map.put(SQLQUERY2, "PARTY_ACCOUNT_REL");
		map.put(SQLQUERY3, "LOAN_MASTER");
		map.put(SQLQUERY4, "LOAN_DETAIL");
		map.put(SQLQUERY5, "ACCOUNT");
		map.put(SQLQUERY6, "LOAN_DETAIL");
		map.put(SQLQUERY7, "LOAN_DETAIL");
		map.put(SQLQUERY8, "PARTY");
		map.put(SQLQUERY9, "PARTY");
		map.put(SQLQUERY10, "PARTY_PARTY_REL");
		map.put(SQLQUERY11, "PARTY");
		map.put(SQLQUERY12, "PARTY_ACCOUNT_REL");
		map.put(SQLQUERY13, "LOAN_MASTER");
		map.put(SQLQUERY14, "PARTY");
		return map.get(sql);

	}

	public static String getSrcFile(String sql) {
		Map<String, String> map = new HashMap<String, String>();
		map.put(SQLQUERY1, "TRANSACTION");
		map.put(SQLQUERY2, "ACCOUNT");
		map.put(SQLQUERY3, "LOAN_DETAIL");
		map.put(SQLQUERY4, "LOAN");
		map.put(SQLQUERY5, "LOAN");
		map.put(SQLQUERY6, "LOAN_COLLATERAL");
		map.put(SQLQUERY7, "LOAN_GUARANTOR");
		map.put(SQLQUERY8, "TRANSFER");
		map.put(SQLQUERY9, "TRANSFER");
		map.put(SQLQUERY10, "PARTY");
		map.put(SQLQUERY11, "PARTY_PARTY_REL");
		map.put(SQLQUERY12, "TRANSACTION");
		map.put(SQLQUERY13, "LOAN_DETAIL");
		map.put(SQLQUERY14, "PARTY_PARTY_REL");
		return map.get(sql);

	}

	public static StringBuilder getFileNameDetail(StringBuilder fileName, String currentQuery)
			throws NullPointerException {
		if (SQLQUERY1.equals(currentQuery)) {
			fileName.append("_01交易明細檔帳號在帳戶檔無資料_");
			return fileName;
		} else if (SQLQUERY2.equals(currentQuery)) {
			fileName.append("_02帳戶檔客戶在顧客主檔無資料_");
			return fileName;
		} else if (SQLQUERY3.equals(currentQuery)) {
			fileName.append("_03批覆書額度主檔在顧客主檔無資料_");
			return fileName;
		} else if (SQLQUERY4.equals(currentQuery)) {
			fileName.append("_04放款主檔在額度檔無資料_");
			return fileName;
		} else if (SQLQUERY5.equals(currentQuery)) {
			fileName.append("_05放款主檔帳號在帳戶檔無資料_");
			return fileName;
		} else if (SQLQUERY6.equals(currentQuery)) {
			fileName.append("_06擔保品檔在額度檔無資料_");
			return fileName;
		} else if (SQLQUERY7.equals(currentQuery)) {
			fileName.append("_07保證人檔在額度檔無資料_");
			return fileName;
		} else if (SQLQUERY8.equals(currentQuery)) {
			fileName.append("_08外幣現鈔買賣檔在顧客主檔無資料_");
			return fileName;
		} else if (SQLQUERY9.equals(currentQuery)) {
			fileName.append("_09境外交易檔在顧客主檔無資料_");
			return fileName;
		} else if (SQLQUERY10.equals(currentQuery)) {
			fileName.append("_10顧客主檔非本行客戶在關係人檔無資料_");
			return fileName;
		} else if (SQLQUERY11.equals(currentQuery)) {
			fileName.append("_11關係人檔客戶在顧客主檔無資料_");
			return fileName;
		} else if (SQLQUERY12.equals(currentQuery)) {
			fileName.append("_12交易明細檔帳戶在顧客主檔非本行客戶資料_");
			return fileName;
		} else if (SQLQUERY13.equals(currentQuery)) {
			fileName.append("_13批覆書額度主檔帳戶在顧客主檔非本行客戶資料_");
			return fileName;
		} else if (SQLQUERY14.equals(currentQuery)) {
			fileName.append("_14關係人檔客戶在顧客主檔無實質受益人資料_");
			return fileName;
		}
		return fileName;
	}

	public static int getPreparedStatementNum(String currentQuery) {

		if (SQLQUERY1.equals(currentQuery) || SQLQUERY8.equals(currentQuery) || SQLQUERY9.equals(currentQuery))
			return 2;

		return 1;
	}

	public static String[] getEmptyHead(String currentQuery) {
		String[] sqlquery1 = { "檔案日期", "業務別", "本會代號", "帳號", "交易日期", "交易執行分行", "交易類別" };
		String[] sqlquery2 = { "檔案日期", "業務別", "本會代號", "客戶統編", "帳號", "帳戶類別", "帳戶狀態" };
		String[] sqlquery3 = { "檔案日期", "業務別", "本會代號", "客戶統編", "額度編號", "額度核准日", "批覆書編號", "批覆書申請日" };
		String[] sqlquery4 = { "檔案日期", "業務別", "本會代號", "放款帳號", "額度編號", "初貸日", "放款狀態" };
		String[] sqlquery5 = { "檔案日期", "業務別", "本會代號", "放款帳號", "額度編號", "初貸日", "放款狀態" };
		String[] sqlquery6 = { "檔案日期", " 業務別", "本會代號", "擔保品編號", "額度編號" };
		String[] sqlquery7 = { "檔案日期", "業務別", "本會代號", "保證人統編", "額度編號" };
		String[] sqlquery8 = { "檔案日期", "業務別", "本會代號", "交易編號", "客戶統編", "實際交易時間", "交易類別" };
		String[] sqlquery9 = { "檔案日期", "業務別", "本會代號", "匯款編號", "客戶統編", "實際匯款時間", "交易類別" };
		String[] sqlquery10 = { "檔案日期", "業務別", "本會代號", "歸屬本分會代號", "客戶統編", "是否本行客戶" };
		String[] sqlquery11 = { "檔案日期", "業務別", "本會代號", "顧客1統編", "顧客1名稱", "顧客2統編", "顧客2名稱", "顧客關係種類代碼", "代碼名稱" };
		String[] sqlquery12 = { "業務別", "本會代號", "帳號", "歸屬本分會代號", "客戶統編", "是否本行客戶" };
		String[] sqlquery13 = { "業務別", "本會代號", "額度編號", "額度核准日", "批覆書編號", "批覆書申請日", "歸屬本分會代號", "客戶統編", "是否本行客戶" };
		String[] sqlquery14 = { "檔案日期", "業務別", "本會代號", "顧客1統編", "顧客1名稱", "顧客2統編", "顧客2名稱", "顧客關係種類代碼", "代碼名稱" };

		if (SQLQUERY1.equals(currentQuery)) {
			return sqlquery1;
		} else if (SQLQUERY2.equals(currentQuery)) {
			return sqlquery2;
		} else if (SQLQUERY3.equals(currentQuery)) {
			return sqlquery3;
		} else if (SQLQUERY4.equals(currentQuery)) {
			return sqlquery4;
		} else if (SQLQUERY5.equals(currentQuery)) {
			return sqlquery5;
		} else if (SQLQUERY6.equals(currentQuery)) {
			return sqlquery6;
		} else if (SQLQUERY7.equals(currentQuery)) {
			return sqlquery7;
		} else if (SQLQUERY8.equals(currentQuery)) {
			return sqlquery8;
		} else if (SQLQUERY9.equals(currentQuery)) {
			return sqlquery9;
		} else if (SQLQUERY10.equals(currentQuery)) {
			return sqlquery10;
		} else if (SQLQUERY11.equals(currentQuery)) {
			return sqlquery11;
		} else if (SQLQUERY12.equals(currentQuery)) {
			return sqlquery12;
		} else if (SQLQUERY13.equals(currentQuery)) {
			return sqlquery13;
		} else if (SQLQUERY14.equals(currentQuery)) {
			return sqlquery14;
		}

		return null;
	}
}
