//package edu.utdallas.database;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SelectQuery {
	
	public static final int leftMostPageNumber = 0;
	public static final int pageSize = 512;
	
	public static void parseQueryCommand(String userCommand) throws IOException {
		
		// Read the data from all the pages (Brute force)		
		List<Map<String,String>> results;
		
		// projected columns from the userCommands;
		List<String> projAttrs = new ArrayList<String>();
		
		String meta_columns = "davisbase_columns";
		String meta_table = "davisbase_tables";
		
		String tableName;
		RandomAccessFile raf_content = null;
		RandomAccessFile raf_meta = new RandomAccessFile("data/"+meta_columns+".tbl", "rw");
		
		// split the command by "from"
		ArrayList<String> commandTokens = removeSpace((new ArrayList<String>(Arrays.asList(userCommand.split("from")))));
		// parse the command of "SHOW TABLES;"
		ArrayList<String> beforeFrom = removeSpace((new ArrayList<String>(Arrays.asList(commandTokens.get(0).split("\\s+")))));
		
		if (beforeFrom.get(0).toLowerCase().equals("show")) {
			
			
			tableName = meta_table;
			File directory = new File("data");

			if (!(new File(directory, tableName + ".tbl").exists())) {
				System.out.println("Table " + tableName + " does not exist! ");
				raf_meta.close();
				return;
			}

			try {
				raf_content = new RandomAccessFile("data/"+tableName+".tbl", "rw");
			} catch (Exception e) {
				System.out.println(e);
			}
			
			String[] columnNames = new String[1];
			columnNames[0] = "show_tables";
			
			results = readFile(raf_content, raf_meta, tableName);
			
			projAttrs.add("*");
			OperationUtils.displayQuery(results, columnNames, null, null, null, null, projAttrs);

		} else {
			// Get the table name;
			tableName = getSelectTableName(userCommand);
			
			File directory = new File("data");

			if (!(new File(directory, tableName + ".tbl").exists())) {
				System.out.println("Table " + tableName + " does not exist! ");
				raf_meta.close();
				return;
			}
			
			try {
				raf_content = new RandomAccessFile("data/"+tableName+".tbl", "rw");
			} catch (Exception e) {
				System.out.println("Table " + tableName + " does not exist! ");
			}
			
			String[] columnNames = getColumnNamesOrTypes(raf_meta, tableName, true);
			String[] dataTypes = getColumnNamesOrTypes(raf_meta, tableName, false);
			
			// Get the projected columns, put them into the list;
			projAttrs = removeSpace(((new ArrayList<String>(Arrays.asList(commandTokens.get(0).split(",|\\s+"))))));
			projAttrs.remove(0);
			
			results = readFile(raf_content, raf_meta, tableName);
			
			ArrayList<String> afterFrom = removeSpace(new ArrayList<String>(Arrays.asList(commandTokens.get(1).split("where"))));
			
			if (afterFrom.size() == 1) {
				OperationUtils.displayQuery(results, columnNames, dataTypes, null, null, null, projAttrs);
			} else {
				List<ArrayList<String>> conditions = getSelectConditions(afterFrom);
				ArrayList<String> condAttrs = conditions.get(0);
				ArrayList<String> operators = conditions.get(1);
				ArrayList<String> condValues = conditions.get(2);
				
				OperationUtils.displayQuery(results, columnNames, dataTypes, condAttrs, operators, condValues, projAttrs);
			}

		}
	
	}
	
	public static String[] getColumnNamesOrTypes (RandomAccessFile raf_content, String tbl_Name, boolean wantColumnNames)
			throws IOException {
		
		// get column names or dataTypes;
		List<String> columnNames = new ArrayList<String>();
		List<String> dataTypes = new ArrayList<String>();
		
		int siblingPageNumber = 0X00000000;
		int siblingOffset = 0X00000000;
		
		while(true)  {
			
			try {
				raf_content.seek(siblingOffset);
			} catch (Exception e) {
				System.out.println(e);
			}
			
			List<String> columnNameBuffer = new ArrayList<String>();
			List<String> dataTypeBuffer = new ArrayList<String>();
			
			// 1. get the record position (bottom up)
			raf_content.readShort();
			int recordNum = raf_content.readShort();
			
			// 2. Get the page number of the right sibling page and check whether it is the rightmost page or not;
			raf_content.skipBytes( 2 );
			
			// get the right sibling page;
			
			int neighborPageNumer = raf_content.readInt();
			
//			Go to the second line to get the record positions;
			raf_content.skipBytes(6);
			
			String[] recordPositions = new String[recordNum];
			for(int i = 0; i < recordNum; i++) {
				int record = 0X00;
				for(int j = 0; j < 2; j++) {
					byte a = raf_content.readByte();
					record = (record << 8) | a & 0XFF;
				}
				recordPositions[i] = String.format("%04X", record);
			}
			
			// 2. get the table name of each column, if the table name is what we want, get the column name and store it
			//	  if not, continue to the next position;

			for (int i = 0; i < recordNum; i++) {
				
				// move the pointer to the ith record
				raf_content.seek(siblingOffset + Long.parseLong(recordPositions[i], 16));
				
				// skip the cell header (Because we have obtained the information )
				raf_content.skipBytes(6);
				
				// get (skip) the 1 byte column num
				int columnNum = raf_content.readByte();
				
				// Get the table name length;
				int tableNameLength = raf_content.readByte() - 12;
				// Get the column name length;
				int columnNameLength = raf_content.readByte() - 12;
				
				// Get the column type length;
				int columnTypeLength = raf_content.readByte() - 12;
				
				// Skip the next three bytes (original_position & is_nullable & unique);
				raf_content.skipBytes(3);
				
				// get the table name;
				String tableName = getValue(raf_content, tableNameLength, true);
				if (tableName.equalsIgnoreCase(tbl_Name)) {
					
					if (wantColumnNames) {
						columnNameBuffer.add(getValue(raf_content, columnNameLength, true));
					}else {
						raf_content.skipBytes(columnNameLength);
						String dataType = getValue(raf_content, columnTypeLength, true);
						dataTypeBuffer.add(dataType);
					}
				}
			}
			
			siblingPageNumber = neighborPageNumer;

			siblingOffset = ( siblingPageNumber - leftMostPageNumber ) * pageSize;
			
			columnNames.addAll(columnNameBuffer);
			dataTypes.addAll(dataTypeBuffer);
			
			if (siblingPageNumber == 0XFFFFFFFF) {
				break;
			}
		} 
		
		return wantColumnNames? columnNames.toArray(new String[0]) : dataTypes.toArray(new String[0]);
	}

	public static List<Map<String,String>> readFile(RandomAccessFile raf_content,RandomAccessFile raf_meta, String tableName) throws IOException {
		
		int meta_offset = 0X00000000;
		raf_meta.seek(meta_offset);
		
		// get the column names;
		String[] columnNames = getColumnNamesOrTypes(raf_meta, tableName, true);
		
		int siblingPageNumber = 0;
		int siblingOffset = 0;
		List<Map<String , String>> results = new ArrayList<>();
		
		// each page
		while(true)  {
//			record is one record;
			try {
				raf_content.seek(siblingOffset);
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println(e);
			}
			
			List<Map<String , String>> page = new ArrayList<>();
			
			try {
				// get the table leaf page (first byte)
				byte leafPage = raf_content.readByte();
				
				// get the number of records
				raf_content.readByte();
				int records = 0X00;
				for(int i = 0; i < 2; i++) {
					byte a = raf_content.readByte();
					records = (records << 8) | (a & 0XFF);
				}

				// get the first record offset
				int firstOffset = 0X00;
				for(int i = 0; i < 2; i++) {
					byte a = raf_content.readByte();
					firstOffset = (firstOffset << 8) | (a & 0XFF);
				}
				
				// get the right sibling page;
				int neighborPageNumber = raf_content.readInt();
				
				siblingPageNumber = neighborPageNumber;
				
				// Get the parent page;
				int parent = 0X00;
				for(int i = 0; i < 4; i++) {
					byte a = raf_content.readByte();
					parent = (parent << 8) | a & 0XFF;
				}
				
				// Skip the two unused bytes
				raf_content.readShort();
				
				// get an array of the positions of the records;
				String[] recordPositions = new String[records];

				for(int i = 0; i < records; i++) {
					int position = 0X00;
					for(int j = 0; j < 2; j++) {
						byte a = raf_content.readByte();
						position = (position << 8) | a & 0XFF;
					}
					recordPositions[i] = String.format("%04X", position);
				}

				// get the cell header and the record body for each record;
				
				// process each record;
				for(int i = 0; i < records; i++) {
					raf_content.seek( siblingOffset + Long.parseLong(recordPositions[i], 16));
					
					// get the payload size 		( 2 Bytes )
					int payload = 0X00;
					for(int j = 0; j < 2; j++) {
						byte a = raf_content.readByte();
						payload = (payload << 8) | a & 0XFF;
					}
					
					// get the rowId 				( 4 Bytes )
					int rowId = 0X00;
					for(int j = 0; j < 4; j++) {
						byte a = raf_content.readByte();
						rowId = (rowId << 8) | a & 0XFF;
					}
					
					// get the number of columns	( 1 Byte  )
					byte numColumns = raf_content.readByte();
					
					// Make a list to store the data types;
					String[] dataTypes = new String[numColumns];
					for(int j = 0; j < numColumns; j++) {
						byte a = raf_content.readByte();
						dataTypes[j] = String.format("%02X", a);
					}
					
					
					
					// Make a map the store the all the records, key is the column name, value is the column content;
					Map<String, String> columns = new LinkedHashMap<String,String>();
					
					// process each column;
					for(int j = 0; j < numColumns; j++) {
						
						String columnName = columnNames[j+1];
						
						String dataType = dataTypes[j];
						String columnValue = "";
						
						switch (dataType) {
						case "00":
							// Null -> 0
							columnValue = "NULL";
							break;
						case "01":
							// TINYINT -> 1
							byte bt = raf_content.readByte();
							columnValue = String.format("%02X", bt);
							break;
						case "02":
							// SMALLINT -> 2
							columnValue = getValue(raf_content, 2, false);
							break;
						case "03":
							// INT -> 4
							columnValue = getValue(raf_content, 4, false);
							
							break;
						case "04":
							// BIGINT, LONG -> 8
							columnValue = getValue(raf_content, 8, false);
							break;
						case "05":
							// Float -> 4
//							columnValue = getValue(raf_content, 4, false);
							float ft = raf_content.readFloat();
							columnValue = String.valueOf(ft);
							break;
						case "06":
							// Double -> 8
							//columnValue = getValue(raf_content, 8, false);
							double val=raf_content.readDouble();
							columnValue = String.valueOf(val);
							break;
						case "08":
							// Year -> 1
							// 2000 should be defined as a final number;
							int year = Integer.parseInt(getValue(raf_content, 1, false)) + 2000;
							columnValue = String.valueOf(year);
							break;

						case "09":
							// TIME -> 4
							int time = raf_content.readInt();
							// read the time into int, (convert it to String to display, if needed convert it to int for comparison)
							columnValue = String.valueOf(time);
							break;
						case "0A":
							// DATETIME -> 8
							long dateTime = raf_content.readLong();
							columnValue = String.valueOf(dateTime);
							break;
						case "0B":
							// DATE -> 8
							long date = raf_content.readLong();
							columnValue = String.valueOf(date);
							break;
						default:
							// TEXT;
							int textNum = Integer.parseInt(dataType, 16) - 12;
							columnValue = getValue(raf_content, textNum, true);
							break;
						}
						
						// put one column into one record;
						columns.put(columnName.toLowerCase(), columnValue);
						
					}
					
					// add each record into one page;
					page.add(columns);
				}
				
				// add one page to the results;
				results.addAll(page);
				
			} catch (IOException e) {
				System.out.println(e);
				return new ArrayList<Map<String,String>>();
			}
			siblingOffset = ( siblingPageNumber - leftMostPageNumber ) * pageSize;
			if (siblingPageNumber == 0XFFFFFFFF) {
				break;
			}
		}
		
		return results;
	}
		
	public static String getSelectTableName (String userCommand) {
		
		ArrayList<String> commandTokens = removeSpace(new ArrayList<String>(Arrays.asList(userCommand.split("from"))));
		ArrayList<String> afterFrom = removeSpace(new ArrayList<String>(Arrays.asList(commandTokens.get(1).split("where"))));
		String tableName = afterFrom.get(0).trim();
		
		return tableName;
	}
	
	// Get the WHERE conditions;
	public static List<ArrayList<String>> getSelectConditions(ArrayList<String> conditions) {
		
		ArrayList<String> conds = removeSpace(new ArrayList<String>(Arrays.asList(conditions.get(1).split("and"))));
 
		ArrayList<String> condAttrs = new ArrayList<String>();
		ArrayList<String> operators = new ArrayList<String>();
		ArrayList<String> condValues = new ArrayList<String>();
		List<ArrayList<String>> results = new ArrayList<>();
		
		for(int i = 0; i < conds.size(); i++) {
			// get the operator
			String operator = conds.get(i).replaceAll("[a-zA-Z|\\d+|\\s+|_]","").trim();
			operators.add(operator);
			// get the operands
			ArrayList<String> c = removeSpace(new ArrayList<String>(Arrays.asList(conds.get(i).split("=|>|>=|<=|!=|\\s+"))));
			condAttrs.add(c.get(0));
			// NOTE: If the column is String type, the commands will have " ", which should be removed;
			condValues.add(c.get(1));
		}
		results.add(condAttrs);
		results.add(operators);
		results.add(condValues);

		return results;
	}
	
	// get the column value as a String;
	public static String getValue(RandomAccessFile raf_content, int num, boolean usingUTF) throws IOException {

		if (usingUTF) {
			byte[] bt = new byte[num];
			for (int i = 0; i < num; i++) {
				bt[i] = raf_content.readByte();
			}
			return(new String(bt, "UTF-8"));
		} else {
			int number = 0X00;
			for(int i = 0; i < num; i++) {
				number = number << 8 | raf_content.readByte();
			}
			return String.format("%04d", number);
		}
	}
	
	// Remove the spaces;
	public static ArrayList<String> removeSpace(ArrayList<String> strings) {
		ArrayList<String> newList = new ArrayList<String>();
		for(String string : strings) {
			if (string != null && string.length() != 0 && string != "" && string != " ") {
				newList.add(string);
			}
		}
		return newList;
	}
}
