package Profile;

import Tool.ETL_Tool_DES;

public class ETL_Profile {
	// ETL系列程式   設定檔  2017.12.07 TimJhang
	
	// **E階段設定參數
	
	// 基本檢核參數(開/關)
	public final static boolean BasicCheck = true;
	// 比對檢核參數(開/關)
	public final static boolean AdvancedCheck = true;
	
	// **連線DB2 設定參數(預計有 7 + 1組)
	
	// Driver, Url, User, Password
	public final static String db2Driver = "com.ibm.db2.jcc.DB2Driver";
	private final static String db2SPSchema = "ETLUSR";
	public final static String db2TableSchema = "ETLUSR";
//	private final static String db2SPSchema = ETL_Tool_DES.decrypt("f36ddb910a7dafa79b9ec0da15a003be");
//	public final static String db2TableSchema = ETL_Tool_DES.decrypt("f36ddb910a7dafa79b9ec0da15a003be");
	
	public final static String db2Url = 
			"jdbc:db2://172.18.21.206:50000/ETLDB001:" +
			"currentschema=" + db2SPSchema + ";" +
			"currentFunctionPath=" + db2SPSchema + ";";
//	public final static String db2User = "ETLUSR";
//	public final static String db2Password = "1qazXSW@";
//	public final static String db2Url = 
//			"jdbc:db2://localhost:50000/sample:" +
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
//	public final static String db2User = "ETLUSR";
//	public final static String db2Password = "1qaz@WSX";
	
//	public final static String db2Url = 
//			"jdbc:db2://172.18.6.151:50000/ETLDB001:" +
//			"currentschema=" + db2SPSchema + ";" +
//			"currentFunctionPath=" + db2SPSchema + ";";
	
//	public final static String db2Url = 
//			"jdbc:db2://172.18.6.151:50000/ETLDB001:currentschema=ETLUSR;currentFunctionPath=ETLUSR;";
	public final static String db2User = "ETLUSR";
	public final static String db2Password = "1qazXSW@";
	
//	public final static String db2Url = 
////			ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c44ab4f8a6772d57e1925e"
////					+ "5a9ab1fb66c8f838f6065c51203dfc32ca630f22f8fa13357e3eaf2e8d8fe2e1770317"
////					+ "06369df75bae18dd9fb36d3b4f37cefd0fe16de0056e6b66c196185635c9acaa060aadceac2b82d800219b");
//			ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c46d3311bad199a52e925e5a"
//					+ "9ab1fb66c83265187a99066055fc32ca630f22f8fa13357e3eaf2e8d8fe2e1770317063"
//					+ "69df75bae18dd9fb36d3b4f37cefd0fe16de0056e6b66c196185635c9acaa060aadceac2b82d800219b");
	
//	public final static String db2User = ETL_Tool_DES.decrypt("2772602b4cd74bec30a2869c8e5426a6");
//	public final static String db2Password = ETL_Tool_DES.decrypt("2d8f563ca3253c20c53af1a4a004b34f");
	
	// Error Log寫入域值
	public final static int ErrorLog_Stage = 10000;
	
	// Data 寫入域值
	public final static int Data_Stage = 10000;
	
	// 業務別
	public final static String Foreign_Currency = "FR"; // 外幣
	
	// 難字表excel檔存放路徑
//	public final static String DifficultWords_Lists_Path = "C:/AML_TOOL/DifficultWords/%s.xlsx";
	
	public final static String DifficultWords_Lists_Path = 
			ETL_Tool_DES.decrypt("8ba6ba4bf4a45186b96fcc12bd0144"
					+ "5545021e9d1fe4431eae94124ee542db4e160dd19b9ddd3c9a");

	// 特殊符號及罕見字表excel檔存放路徑
//	public final static String SpecialWords_Lists_Path = "C:/AML_TOOL/SpecialBig5Words/SpecialBig5Words.xlsx";
	
	public final static String SpecialWords_Lists_Path = 
			ETL_Tool_DES.decrypt("8ba6ba4bf4a45186a3850695ef0ab3b2c"
					+ "085821dab27407e0ff582606aa619f62581dc1e31994710893522724e52e2a9160dd19b9ddd3c9a");
	
	// 連線GAML用URL string
//	public final static String GAML_db2User = "GAMLETL";
//	public final static String GAML_db2Password = "1qaz@WSX";
//	public final static String GAML_db2TableSchema = "SRC";
//	private final static String GAML_db2SPSchema = "SRC";
//	public final static String db2UrlGAMLpre = "jdbc:db2://172.18.6.133:50000/GAML";
//	public final static String db2UrlGAMLafter = 
//			":currentschema=" + GAML_db2SPSchema + ";" +
//			"currentFunctionPath=" + GAML_db2SPSchema + ";";
	
//	public final static String GAML_db2User = ETL_Tool_DES.decrypt("0f62db871d1d0101");
//	public final static String GAML_db2Password = ETL_Tool_DES.decrypt("44ee592ddae3f0d2c53af1a4a004b34f");
//	public final static String GAML_db2TableSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
//	private final static String GAML_db2SPSchema = ETL_Tool_DES.decrypt("29ee04b861a87da1");
//	public final static String db2UrlGAMLpre = 
//			ETL_Tool_DES.decrypt("58fcd8a8d6fa0357b43596ad4e09a6c4493ff949a610bfc777432c83a5696d76a4a51f2d50ba1358");
//	public final static String db2UrlGAMLafter = 
//			ETL_Tool_DES.decrypt("210854254ff94eec32f326325a431d9a248f31040be3a"
//					+ "7193b4f37cefd0fe16ddafed100689a1ac85961605e87949653");
	
	public final static String GAML_db2User = "GAMLETL";
	public final static String GAML_db2Password = "1qaz@WSX";
	public final static String GAML_db2TableSchema = "SRC";
	private final static String GAML_db2SPSchema = "SRC";
	public final static String db2UrlGAMLpre = "jdbc:db2://172.18.21.207:50000/GAML";
	public final static String db2UrlGAMLafter = 
			":currentschema=" + GAML_db2SPSchema + ";" +
			"currentFunctionPath=" + GAML_db2SPSchema + ";";
	
	// GAMLDB
//	public static String getGAML_DB2Url() {
////		String db2url = "jdbc:db2://" + db2Ip + ":" + db2port + "/GAMLDB:" + "currentschema=" + db2SPSchema + ";"
////				+ "currentFunctionPath=" + db2SPSchema + ";";
//		String db2url = "jdbc:db2://172.18.6.133:50000/GAMLDB:" + "currentschema=" + GAML_db2SPSchema + ";"
//				+ "currentFunctionPath=" + GAML_db2SPSchema + ";";
//		return db2url;
//	}
	
	// 新北市農會附設北區農會電腦共用中心  951  相關參數
	
	// 財團法人農漁會南區資訊中心  952  相關參數
	
	// 板橋區農會電腦共用中心  928  相關參數
	
	// 財團法人農漁會聯合資訊中心  910  相關參數
	
	// 高雄市農會  605  相關參數
	
	// 農漁會資訊共用系統  600  相關參數
	
	// 018金庫  018  相關參數
	
	// 各資料讀檔緩衝區大小
//	public final static int ETL_E_PARTY = 620;
//	public final static int ETL_E_PARTY_PARTY_REL = 203;
//	public final static int ETL_E_PARTY_PHONE = 43;
//	public final static int ETL_E_PARTY_ADDRESS = 137;
//	public final static int ETL_E_ACCOUNT = 113;
//	public final static int ETL_E_TRANSACTION = 484;
//	public final static int ETL_E_LOAN_DETAIL = 112;
//	public final static int ETL_E_LOAN = 214;
//	public final static int ETL_E_COLLATERAL = 203;
//	public final static int ETL_E_GUARANTOR = 124;
//	public final static int ETL_E_FX_RATE = 30;
//	public final static int ETL_E_SERVICE = 1434;
//	public final static int ETL_E_TRANSFER = 723;
//	public final static int ETL_E_FCX = 251;
//	public final static int ETL_E_CALENDAR = 23;
	public final static int ETL_E_PARTY = 310;
	public final static int ETL_E_PARTY_PARTY_REL = 101;
	public final static int ETL_E_PARTY_PHONE = 22;
	public final static int ETL_E_PARTY_ADDRESS = 63;
	public final static int ETL_E_ACCOUNT = 56;
	public final static int ETL_E_TRANSACTION = 242;
	public final static int ETL_E_TRANSACTION_OLD = 204;
	public final static int ETL_E_LOAN_DETAIL = 56;
	public final static int ETL_E_LOAN = 107;
	public final static int ETL_E_COLLATERAL = 101;
	public final static int ETL_E_GUARANTOR = 62;
	public final static int ETL_E_FX_RATE = 15;
	public final static int ETL_E_SERVICE = 717;
	public final static int ETL_E_TRANSFER = 361;
	public final static int ETL_E_FCX = 125;
	public final static int ETL_E_CALENDAR = 11;
	public final static int ETL_DM_ACCTMAPPING_LOAD = 40;
	public final static int ETL_DM_BRANCHMAPPING_LOAD = 12;
	public final static int ETL_DM_IDMAPPING_LOAD = 40;
	public final static int ETL_E_AGENT = 85;
	public final static int ETL_E_SCUSTBOX = 30;
	public final static int ETL_E_SCUSTBOXOPEN =379;
	public final static int ETL_E_SPARTY = 180;

	// 讀檔筆數域值
	public final static int ETL_E_Stage = 10000;
	
	// DM
	public final static String ETL_DM_SAVEPATH = "C:\\ETL\\DM";
	
}
