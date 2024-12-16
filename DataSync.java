import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSync {

	private static final String LAST_SYNC_FILE = "/data/dbsync/last_sync_time.txt";
	private static final String DATE_FORMAT = "yyyyMMddHHmmss";
	
	private static final String UAT_DB_URL = "";
	private static final String TRA_DB_URL = "";
	private static final String USER = "";
	private static final String PASSWORD = "";
	private static final int BATCH_SIZE = 500;
	private static final List<String> SCHEMAS = Arrays.asList("PTLIUSR", "PTLEUSR"); 
	//, "ESWIUSR", "ESWEUSR", "CAGIUSR", "CAGEUSR", "CLRIUSR", "CLREUSR", "ECTIUSR", "ECTEUSR", "RKMIUSR"); // 스키마 목록 지정
	private static final List<String> EXCLUDED = Arrays.asList("CAGIUSR.CGMI_TRKNG_CNTR_PRCSS_R"); // Collections.EMPTY_LIST; //
	
	private static final List<String> INCLUDED = Arrays.asList(
		"PTLIUSR.COMI_BLTBRD_MGMT_M",
		"PTLIUSR.COMI_BLTBRD_TP_CD_D",
		"PTLIUSR.COMI_BTCH_M",
		"PTLIUSR.COMI_BTCH_PRMT_D",
		"PTLIUSR.COMI_BTCH_SCHD_M",
		"PTLIUSR.COMI_BTCH_SCHD_PRMT_D",
		"PTLIUSR.COMI_CNTY_CD_LANG_M",
		"PTLIUSR.COMI_CNTY_CD_M",
		"PTLIUSR.COMI_COMN_CD_D",
		"PTLIUSR.COMI_COMN_CD_LANG_D",
		"PTLIUSR.COMI_COMN_CD_LANG_M",
		"PTLIUSR.COMI_COMN_CD_M",
		"PTLIUSR.COMI_CSTM_LANG_M",
		"PTLIUSR.COMI_CSTM_M",
		"PTLIUSR.COMI_CSTM_ORGZ_CD_LANG_M",
		"PTLIUSR.COMI_CSTM_ORGZ_CD_M",
		"PTLIUSR.COMI_CSTM_ORGZ_M",
		"PTLIUSR.COMI_CSTM_TP_D",
		"PTLIUSR.COMI_DOC_CD_LANG_M",
		"PTLIUSR.COMI_DOC_CD_M",
		"PTLIUSR.COMI_DOC_DCLA_CO_TP_M",
//		"PTLIUSR.COMI_EXRT_M",
		"PTLIUSR.COMI_HLDY_M",
		"PTLIUSR.COMI_LBL_LANG_M",
		"PTLIUSR.COMI_LBL_M",
		"PTLIUSR.COMI_MANL_M",
		"PTLIUSR.COMI_MENU_LANG_M",
		"PTLIUSR.COMI_MENU_M",
		"PTLIUSR.COMI_MENU_SCRN_D",
		"PTLIUSR.COMI_MSG_LANG_M",
		"PTLIUSR.COMI_MSG_M",
		"PTLIUSR.COMI_REGN_CD_LANG_M",
		"PTLIUSR.COMI_REGN_CD_M",
		"PTLIUSR.COMI_RELA_BLTBRD_D",
		"PTLIUSR.COMI_SCRN_DOC_CD_LANG_M",
		"PTLIUSR.COMI_SCRN_DOC_CD_M",
		"PTLIUSR.COMI_SCRN_LANG_M",
		"PTLIUSR.COMI_SCRN_M",
//		"PTLIUSR.COMI_VALDN_CTRA_D",
//		"PTLIUSR.COMI_VALDN_CTRA_M",
		"PTLIUSR.COMI_VHCL_BODY_TP_CD_M",
		"PTLIUSR.COMI_VHCL_CLR_CD_M",
		"PTLIUSR.COMI_VHCL_CTGR_CD_M",
		"PTLIUSR.COMI_VHCL_FL_TP_CD_M",
		"PTLIUSR.COMI_VHCL_HLPN_CTGR_CD_M",
		"PTLIUSR.COMI_VHCL_IMP_CNTY_CD_M",
		"PTLIUSR.COMI_VHCL_INSR_TP_CD_M",
		"PTLIUSR.COMI_VHCL_MDL_CD_M",
		"PTLIUSR.COMI_VHCL_MDL_NO_CD_M",
		"PTLIUSR.COMI_VHCL_MKER_CD_M",
		"PTLIUSR.COMI_VHCL_PRPL_TP_CD_M",
		"PTLIUSR.COMI_VHCL_TRMSSN_TP_CD_M",
		"PTLIUSR.COMI_VHCL_USE_CD_M",
		"PTLEUSR.COME_MENU_LANG_M",
		"PTLEUSR.COME_MENU_M",
		"PTLEUSR.COME_MENU_SCRN_D",
		"PTLIUSR.COMI_BTBDMS_M",
		"PTLIUSR.COMI_BTBDMS_TP_CD_VAL_D",
		"PTLIUSR.COMI_USR_M",
		"PTLIUSR.COMI_USR_MENU_D",
		"PTLIUSR.COMI_USR_CSTM_D",
		"PTLIUSR.COMI_HR_USR_M",
		"PTLIUSR.COMI_ROLE_M",
		"PTLIUSR.COMI_ROLE_LANG_M",
		"PTLIUSR.COMI_USR_ROLE_D",
		"PTLIUSR.COMI_AUTH_M",
		"PTLIUSR.COMI_AUTH_LANG_M",
		"PTLIUSR.COMI_USR_AUTH_D",
		"PTLIUSR.COMI_ROLE_AUTH_D",
		"PTLIUSR.COMI_AUTH_MENU_D",
		"PTLEUSR.COME_CO_M",
		"PTLEUSR.COME_CO_TP_D",
		"PTLEUSR.COME_BUCN_M",
		"PTLEUSR.COME_ORG_DEPT_M",
		"PTLEUSR.COME_ORG_M",
		"PTLEUSR.COME_USR_CO_D",
		"PTLEUSR.COME_USR_ORG_D",
		"PTLEUSR.COME_USR_M",
		"PTLEUSR.COME_USR_TP_D",
		"PTLEUSR.COME_USR_AUTH_D",
		"PTLEUSR.COME_AUTH_M",
		"PTLEUSR.COME_CO_TP_AUTH_D",
		"PTLEUSR.COME_AUTH_LANG_M",
		"PTLEUSR.COME_USR_MENU_D",
		"PTLEUSR.COME_MENU_M",
		"PTLEUSR.COME_AUTH_MENU_D",
		"PTLEUSR.COME_SCRN_M",
		"PTLEUSR.COME_SCRN_LANG_M"
);

	private static DataSource uatDataSource;
	private static DataSource traDataSource;

	static {
		// HikariCP 설정
		HikariConfig uatConfig = new HikariConfig();
		uatConfig.setJdbcUrl(UAT_DB_URL);
		uatConfig.setUsername(USER);
		uatConfig.setPassword(PASSWORD);
		uatConfig.setMaximumPoolSize(10); // 풀 크기 설정
		uatConfig.setConnectionTimeout(6000000);
		uatDataSource = new HikariDataSource(uatConfig);

		HikariConfig traConfig = new HikariConfig();
		traConfig.setJdbcUrl(TRA_DB_URL);
		traConfig.setUsername(USER);
		traConfig.setPassword(PASSWORD);
		traConfig.setMaximumPoolSize(10); // 풀 크기 설정
		traConfig.setConnectionTimeout(6000000);
		traDataSource = new HikariDataSource(traConfig);
	}

	public static void main(String[] args) throws SQLException, IOException {
		List<String> tables = getTablesToSync();

		int totalTables = tables.size(); // 전체 테이블 수
		AtomicInteger processedTables = new AtomicInteger(0); // 처리된 테이블 수, 쓰레드 안전하게 처리하기 위해 AtomicInteger 사용
		int numThreads = 8; // 쓰레드 개수

		// 고정된 크기의 스레드 풀 생성
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);

		String lastSyncTime = getLastSyncTime();
		String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATE_FORMAT));

		System.out.println("Last Sync Time : " + lastSyncTime);
		System.out.println("Current Time : " + currentTime);
		
		// 접두사와 일치하는 테이블만 동기화
		for (String table : tables) {
			
			if(EXCLUDED.contains(table)) 
				continue;
			
			if(INCLUDED.isEmpty() || INCLUDED.contains(table)) {
				executor.submit(() -> {

					System.out.println("Processing : " + table);

					try {
						syncTableData(table, lastSyncTime);
						int processed = processedTables.incrementAndGet();
	
						// 진행률 계산 및 출력
						double progress = ((double)processed / totalTables) * 100;
						System.out.printf(">>>>>   Progress: %.2f%% (%d/%d tables synchronized)%n", progress, processed, totalTables);
						
						
					} catch (Exception e) {
						System.err.printf("Error syncing table %s: %s%n", table, e.getMessage());
						e.printStackTrace();
					}
				});
			}
		}
		// 스레드 풀 종료
	    executor.shutdown();
	    saveLastSyncTime(currentTime);
	    
	    try {
	        if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
	            System.err.println("Thread pool did not terminate in the specified time.");
	            executor.shutdownNow();
	        }
	    } catch (InterruptedException e) {
	        System.err.println("Thread pool shutdown interrupted: " + e.getMessage());
	        executor.shutdownNow();
	    }
	}

	// 동기화할 테이블 목록
	private static List<String> getTablesToSync() throws SQLException {
		List<String> tables = new ArrayList<>();
		try (Connection connection = traDataSource.getConnection()) {
			DatabaseMetaData metaData = connection.getMetaData();
			String[] types = {"TABLE"};
			for (String schema : SCHEMAS) {
				ResultSet rs = metaData.getTables(null, schema, "%", types);
				while (rs.next()) {
					String tableName = rs.getString("TABLE_NAME");
					// 접두사와 일치하는지 확인
					//if (PREFIX.stream().anyMatch(prefix -> tableName.startsWith(prefix))) {
						String tableFullName = schema + "." + tableName;
						if(INCLUDED.isEmpty() || INCLUDED.contains(tableFullName))
							tables.add(tableFullName);
					//}
				}
			}
		}
		return tables;
	}

	// 테이블 데이터 동기화
	private static void syncTableData(String table, String lastSyncTime) {
		try (Connection uatConnection = uatDataSource.getConnection(); Connection traConnection = traDataSource.getConnection()) {

			System.out.println("Conncted db for [" + table + "]");

			uatConnection.setAutoCommit(false);
			traConnection.setAutoCommit(false);

			// 기본 키 컬럼 조회 (from uat)
			List<String> primaryKeys = getPrimaryKeys(traConnection, table);

			int offset = 0;
			boolean hasMoreData = true;
			do {
				// UAT 테이블에서 1000건씩 데이터 조회
				List<Map<String, Object>> uatData = fetchData(uatConnection, table, offset, lastSyncTime);
				if (uatData.isEmpty()) {
					hasMoreData = false;
					break;
				}

				// TRA 테이블에서 동일한 기본 키 조합으로 조회
				Map<String, Map<String, Object>> traDataMap = fetchDataMapByKeys(traConnection, table, primaryKeys, uatData);

				// 데이터 비교 및 차이점 반영
				processDifferences(uatData, traDataMap, traConnection, table, primaryKeys);

				System.out.println(table + ", " + "offset:" + offset);
				offset += BATCH_SIZE;
				
				traConnection.commit();
			} while (hasMoreData);

			traConnection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// TRA 데이터 조회
	private static List<Map<String, Object>> fetchData(Connection connection, String table, int offset, String lastSyncType) throws SQLException {
		String query = "SELECT * FROM ("
			    + "SELECT /*+ FIRST_ROWS(n) */ a.*, ROWNUM rnum FROM " + table + " a "
				+ "WHERE LAST_MDFY_DTM >= TO_DATE(?, 'YYYYMMDDHH24MISS') "
				+ "AND ROWNUM <= ?) "
				+ "WHERE rnum > ?";
		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.setString(1, lastSyncType);
			stmt.setInt(2, offset + BATCH_SIZE);
			stmt.setInt(3, offset);
			
			ResultSet rs = stmt.executeQuery();
			return resultSetToList(rs);
		} catch (SQLException e) {
			System.err.print(query);
			throw e;
		}
	}

	// UAT 데이터 조회 및 Map으로 변환 (키는 기본 키 조합)
	private static Map<String, Map<String, Object>> fetchDataMapByKeys(Connection connection, String table, List<String> primaryKeys, List<Map<String, Object>> uatData) throws SQLException {
		Map<String, Map<String, Object>> dataMap = new HashMap<>();

		String keyValues = generateKeyValuesForQuery(uatData, primaryKeys);
		String query = "SELECT * FROM " + table + " WHERE (" + String.join(",", primaryKeys) + ") IN (" + keyValues + ")";

		
		
		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = resultSetToMap(rs);
				String key = generateKey(row, primaryKeys);
				dataMap.put(key, row);
			}
		} catch(SQLException e) {
			System.err.println("-------------->" + query);
			throw e;
		}
		return dataMap;
	}

	// 기본 키 조합 생성
	private static String generateKey(Map<String, Object> row, List<String> primaryKeys) {
	    StringBuilder keyBuilder = new StringBuilder();
	    for (String pk : primaryKeys) {
	        ColumnValue columnValue = (ColumnValue) row.get(pk); // ColumnValue 객체 사용
	        Object value = columnValue != null ? columnValue.value : null;
	        keyBuilder.append(value != null ? value.toString() : "NULL").append("-"); // Null 값 처리
	    }
	    return keyBuilder.toString();
	}

	// TRA 데이터를 기반으로 쿼리용 키 값 생성
	private static String generateKeyValuesForQuery(List<Map<String, Object>> uatData, List<String> primaryKeys) {
	    StringBuilder keyValuesBuilder = new StringBuilder();

	    for (Map<String, Object> row : uatData) {
	        keyValuesBuilder.append("(");
	        
	        for (String pk : primaryKeys) {
	            ColumnValue columnValue = (ColumnValue) row.get(pk); // ColumnValue로 캐스팅
	            Object value = columnValue != null ? columnValue.value : null;
	            int type = columnValue != null ? columnValue.type : java.sql.Types.NULL;

	            // 타입에 따라 SQL 표현식 구성
	            if (value == null) {
	                keyValuesBuilder.append("NULL");
	            } else {
	                switch (type) {
	                    case java.sql.Types.VARCHAR:
	                    case java.sql.Types.CHAR:
	                        String strValue = value.toString().replace("'", "''"); // 작은 따옴표 이스케이프
	                        keyValuesBuilder.append("'").append(strValue).append("'");
	                        break;

	                    case java.sql.Types.DATE:
	                        keyValuesBuilder.append("TO_DATE('")
	                            .append(value.toString())
	                            .append("', 'YYYY-MM-DD HH24:MI:SS.FF')");
	                        break;

	                    case java.sql.Types.TIMESTAMP:
	                        keyValuesBuilder.append("TO_TIMESTAMP('")
	                            .append(value.toString())
	                            .append("', 'YYYY-MM-DD HH24:MI:SS.FF')");
	                        break;

	                    case java.sql.Types.INTEGER:
	                    case java.sql.Types.BIGINT:
	                    case java.sql.Types.TINYINT:
	                    case java.sql.Types.SMALLINT:
	                        keyValuesBuilder.append(value);
	                        break;

	                    case java.sql.Types.DOUBLE:
	                    case java.sql.Types.FLOAT:
	                    case java.sql.Types.DECIMAL:
	                        keyValuesBuilder.append(value);
	                        break;

	                    default:
	                        // 기타 타입에 대한 기본 처리
	                        String defaultStrValue = value.toString().replace("'", "''");
	                        keyValuesBuilder.append("'").append(defaultStrValue).append("'");
	                        break;
	                }
	            }
	            keyValuesBuilder.append(", ");
	        }
	        
	        // 마지막 쉼표와 공백 제거
	        keyValuesBuilder.delete(keyValuesBuilder.length() - 2, keyValuesBuilder.length());
	        keyValuesBuilder.append("),");
	    }

	    if (keyValuesBuilder.length() > 0) {
	        keyValuesBuilder.deleteCharAt(keyValuesBuilder.length() - 1); // 마지막 쉼표 제거
	    }

	    return keyValuesBuilder.toString();
	}
	
	// 차이점 처리 (Insert, Update, Delete)
	private static void processDifferences(List<Map<String, Object>> uatData, Map<String, Map<String, Object>> traDataMap, Connection traConnection, String table, List<String> primaryKeys)
	        throws SQLException {

	    int insertedCount = 0;
	    int updatedCount = 0;

	    for (Map<String, Object> uatRow : uatData) {
	        String key = generateKey(uatRow, primaryKeys);
	        Map<String, Object> traRow = traDataMap.get(key);

	        if (traRow == null) {
	            // UAT에만 있는 데이터는 TRA에 Insert
	            insertRow(traConnection, table, uatRow);
	            insertedCount++;
	        } else {
	            // 데이터가 다른 경우 Update
	            if (!areRowsEqual(uatRow, traRow)) {
	                updateRow(traConnection, table, uatRow, primaryKeys);
	                updatedCount++;
	            }
	            traDataMap.remove(key); // 처리된 TRA 데이터는 제거
	        }
	    }

	    System.out.println("...inserted count : " + insertedCount + "...updated count : " + updatedCount);
	}
	
	// 두 행의 데이터를 비교하는 메서드
	private static boolean areRowsEqual(Map<String, Object> row1, Map<String, Object> row2) {
	    if (row1.size() != row2.size()) {
	        return false;
	    }

	    for (String key : row1.keySet()) {
	        ColumnValue value1 = (ColumnValue) row1.get(key);
	        ColumnValue value2 = (ColumnValue) row2.get(key);

	        if (value1 == null && value2 == null) {
	            continue;
	        }
	        if (value1 == null || value2 == null) {
	            return false; // 한쪽만 null인 경우 다름
	        }
	        // 값이 null인 경우를 추가 확인
	        if (value1.value == null && value2.value == null) {
	            continue;
	        }
	        if (value1.value == null || value2.value == null || !value1.value.equals(value2.value)) {
	            return false; // 값이 다를 경우
	        }
	    }
	    return true; // 모든 값이 같음
	}

	// 기본 키 조회
	private static List<String> getPrimaryKeys(Connection connection, String table) throws SQLException {
		List<String> primaryKeys = new ArrayList<>();
		DatabaseMetaData metaData = connection.getMetaData();
		String[] tableParts = table.split("\\.");
		String schema = tableParts[0];
		String tableName = tableParts[1];
		ResultSet rs = metaData.getPrimaryKeys(null, schema, tableName);
		while (rs.next()) {
			primaryKeys.add(rs.getString("COLUMN_NAME"));
		}
		return primaryKeys;
	}

	// Insert 문 생성 및 실행
	private static void insertRow(Connection connection, String table, Map<String, Object> row) throws SQLException {
		String insertSQL = generateInsertSQL(table, row);
		
		// 파라미터 로깅을 위한 StringBuilder
	    StringBuilder logParams = new StringBuilder("Executing Insert: ");
	    logParams.append(insertSQL).append(" | Parameters: ");
	    
		try (PreparedStatement stmt = connection.prepareStatement(insertSQL)) {
			setPreparedStatementParameters(stmt, row, Collections.EMPTY_LIST, logParams);
			
			
			stmt.executeUpdate();
		} catch(SQLException e) {
			System.out.println(logParams.toString());
			throw e;
		}
//		System.out.println(insertSQL);
	}

	// Update 문 생성 및 실행
	private static void updateRow(Connection connection, String table, Map<String, Object> row, List<String> primaryKeys) throws SQLException {
		String updateSQL = generateUpdateSQL(table, row, primaryKeys);
		
		 // 파라미터 로깅을 위한 StringBuilder
	    StringBuilder logParams = new StringBuilder("Executing Update: ");
	    logParams.append(updateSQL).append(" | Parameters: ");
	    
		try (PreparedStatement stmt = connection.prepareStatement(updateSQL)) {
			int paramIndex = setPreparedStatementParameters(stmt, row, primaryKeys, logParams);
			setPrimaryKeyParameters(stmt, row, primaryKeys, paramIndex, logParams);
			
			stmt.executeUpdate();

		} catch(SQLException e) {
			System.out.println(logParams.toString());
			throw e;
		}
//		System.out.println(updateSQL);
	}

	// INSERT SQL 문 동적 생성
	private static String generateInsertSQL(String table, Map<String, Object> row) throws SQLException {
		StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
		StringBuilder values = new StringBuilder("VALUES (");

		for (String column : row.keySet()) {
			sql.append(column).append(", ");
			values.append("?, ");
		}

		sql.delete(sql.length() - 2, sql.length()); // 마지막 쉼표 제거
		values.delete(values.length() - 2, values.length()); // 마지막 쉼표 제거

		sql.append(") ").append(values).append(")");
		return sql.toString();
	}

	// UPDATE SQL 문 동적 생성
	private static String generateUpdateSQL(String table, Map<String, Object> row, List<String> primaryKeys) throws SQLException {
		StringBuilder sql = new StringBuilder("UPDATE ").append(table).append(" SET ");

		for (String column : row.keySet()) {
			if (!primaryKeys.contains(column)) {
				sql.append(column).append(" = ?, ");
			}
		}

		sql.delete(sql.length() - 2, sql.length()); // 마지막 쉼표 제거

		sql.append(" WHERE ");
		for (String pk : primaryKeys) {
			sql.append(pk).append(" = ? AND ");
		}

		sql.delete(sql.length() - 5, sql.length()); // 마지막 'AND' 제거
		return sql.toString();
	}

	// PreparedStatement에 값을 동적으로 바인딩 (타입 포함)
	private static int setPreparedStatementParameters(PreparedStatement stmt, Map<String, Object> row, List<String> primaryKeys, StringBuilder logParams) throws SQLException {
		int index = 1;
		 for (String column : row.keySet()) {
			
			if(primaryKeys.isEmpty() || !primaryKeys.contains(column)) {				
				ColumnValue columnValue = (ColumnValue) row.get(column);
				int columnType = columnValue.type;
				Object value = columnValue.value;
				
				setParameter(stmt, index, columnType, value);
				logParams.append("[").append(index).append(": ").append(value).append("(").append(value!=null ? value.getClass(): "null").append(")").append("], ");
				index++;
			}
			
		}
		return index;
	}

	// 기본 키 컬럼 값만 바인딩 (타입 포함)
	private static void setPrimaryKeyParameters(PreparedStatement stmt, Map<String, Object> row, List<String> primaryKeys, int startIndex, StringBuilder logParams) throws SQLException {
		int index = startIndex;
		for (String pk : primaryKeys) {
			ColumnValue columnValue = (ColumnValue)row.get(pk);
			int columnType = columnValue.type;
			Object value = columnValue.value;

			 // 파라미터 설정 및 로깅
	        setParameter(stmt, index, columnType, value);
	        logParams.append("[").append(index).append(": ").append(value).append("(").append(value!=null ? value.getClass(): "null").append(")").append("], ");
	        index++;
			
		}
	}
	
	private static void setParameter(PreparedStatement stmt, int index, int columnType, Object value) throws SQLException {
		switch (columnType) {
			case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
				stmt.setString(index, (String)value);
				break;
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
				stmt.setInt(index, (Integer)value);
				break;
            case java.sql.Types.DATE:
            	stmt.setDate(index, (java.sql.Date)value);
                break;
            case java.sql.Types.TIMESTAMP:
                if (value instanceof oracle.sql.TIMESTAMP) {
                    // oracle.sql.TIMESTAMP를 java.sql.Timestamp로 변환하여 설정
                    stmt.setTimestamp(index, ((oracle.sql.TIMESTAMP) value).timestampValue());
                } else if (value instanceof java.sql.Timestamp) {
                    stmt.setTimestamp(index, (java.sql.Timestamp) value);
                } else {
                    stmt.setObject(index, value);
                }
                break;  
            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DECIMAL:
				stmt.setDouble(index, (Double)value);
				break;
            case java.sql.Types.BLOB:
                if (value instanceof java.sql.Blob) {
                    // BLOB을 byte[]로 변환하여 설정
                    java.sql.Blob blob = (java.sql.Blob) value;
                    stmt.setBytes(index, blob.getBytes(1, (int) blob.length()));
                } else {
                    stmt.setBytes(index, (byte[]) value); // 이미 byte[]일 경우
                }
                break;
            case java.sql.Types.CLOB:
                if (value instanceof java.sql.Clob) {
                    // CLOB을 String으로 변환하여 설정
                    java.sql.Clob clob = (java.sql.Clob) value;
                    stmt.setString(index, clob.getSubString(1, (int) clob.length()));
                } else {
                    stmt.setString(index, (String) value); // 이미 String일 경우
                }
                break;
            default:
                stmt.setObject(index, value);
		}
	}

	// ResultSet을 Map 형태로 변환 (컬럼 타입 포함)
	private static Map<String, Object> resultSetToMap(ResultSet rs) throws SQLException {
		Map<String, Object> row = new HashMap<>();
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();

		for (int i = 1; i <= columnCount; i++) {
			String columnName = metaData.getColumnName(i);
			int columnType = metaData.getColumnType(i); // 컬럼 타입
			row.put(columnName, new ColumnValue(rs.getObject(i), columnType));
		}
		return row;
	}
	
	// ResultSet을 List<Map> 형태로 변환 (컬럼 타입 포함)
	private static List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
	    List<Map<String, Object>> rows = new ArrayList<>();
	    ResultSetMetaData metaData = rs.getMetaData();
	    int columnCount = metaData.getColumnCount();

	    while (rs.next()) {
	        Map<String, Object> row = new HashMap<>();
	        for (int i = 1; i <= columnCount; i++) {
	        	String columnName = metaData.getColumnName(i);
	        	
	        	if(columnName.equalsIgnoreCase("rnum"))
	        		continue;
	        	
	            int columnType = metaData.getColumnType(i); // 컬럼 타입 저장
	            row.put(columnName, new ColumnValue(rs.getObject(i), columnType)); // ColumnValue 객체로 저장
	        }
	        rows.add(row);
	    }
	    return rows;
	}

	// 컬럼 값과 타입을 저장하는 클래스
	private static class ColumnValue {
		Object value;
		int type;

		ColumnValue(Object value, int type) {
			this.value = value;
			this.type = type;
		}
	}
	
	private static String getLastSyncTime() {
	    File file = new File(LAST_SYNC_FILE);
	    if (!file.exists()) {
	        // 파일이 없으면 기본 시작 날짜 반환
	        return "20241109000000"; // 원하는 기본값으로 설정 
	    }

	    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
	        return reader.readLine().trim();
	    } catch(IOException e) {
	    	return "20241109000000"; 
	    }
	}
	
	private static void saveLastSyncTime(String currentTime) throws IOException {
	    
	    try (BufferedWriter writer = new BufferedWriter(new FileWriter(LAST_SYNC_FILE, false))) {
	        writer.write(currentTime);
	    }
	}
}
