//package edu.utdallas.database;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperationUtils {
	
	// show the expected columns on the screen;
	public static void displayQuery(List<Map<String, String>> results, String[] columnNameArray, String[] dataTypeArray, List<String> condAttrs, 
			List<String> operators, List<String> condValues, List<String> projAttrs) {
	
		int index = 0;
		
		List<String> dataTypeList = null;
		List<String> columnNameList = null;
		
		boolean is_showtables = false;
		if (columnNameArray.length == 1 && columnNameArray[0] == "show_tables") {
			is_showtables = true;
		}else {
			dataTypeList = new ArrayList<String>(Arrays.asList(dataTypeArray));
			columnNameList = new ArrayList<String>(Arrays.asList(columnNameArray));
		}
		
		// number of conditions to check;
		int size ;
		
		for(Map<String, String> map : results) { // each map is one record;
			
			// let a flag to indicate whether a record is valid or not;
			boolean isValid = true;
			
			if (projAttrs.contains("*")) {
				
				// 1. display the headers
				if (index == 0) {
					System.out.print("|");
					for(String columnName : map.keySet()) {
						System.out.print(String.format("%24s", columnName));
					}
					System.out.print("|");
					printDoubleLines(map.size());
				}
				
				// 2. iterate to display all the data
				System.out.print("|");
				
				for(String columnName : map.keySet()) {
					
					if(map.get(columnName).equalsIgnoreCase("NULL")){
						System.out.print(String.format("%24s", "NULL"));
						
						continue;
					}
					
					if (!is_showtables) {
						
						int columnIndex = columnNameList.indexOf(columnName);
						String dataType = dataTypeList.get(columnIndex);
						StringBuilder dateString = new StringBuilder();
						
						switch (dataType) {
						case "DATE":
							long dateLong = Long.parseLong(map.get(columnName));
							Date date = new Date(dateLong);
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							String strDate = dateFormat.format(date);
							System.out.print(String.format("%24s", strDate));
							break;
						case "TIME":
							int timeInt = Integer.parseInt(map.get(columnName));
							timeInt /= 1000;
							dateString.append(timeInt / 3600).append(":").append((timeInt % 3600) / 60).append(":").append(timeInt % 60);
							System.out.print(String.format("%24s", dateString) );
							break;
						case "DATETIME":
							long dateTimeLong = Long.parseLong(map.get(columnName));
							Date dateTimeDate = new Date(dateTimeLong);
							DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
							String strDateTime = dateTimeFormat.format(dateTimeDate);
							System.out.print(String.format("%24s", strDateTime));
							break;
						default:
							System.out.print(String.format("%24s", map.get(columnName)));
							break;
						}
						
					}else {
						System.out.print(String.format("%24s", map.get(columnName)));
					}
					
					
				}
				System.out.print("|");
				printSingleLines(map.size());
				
				index++;
				
			} else {

				size = (condAttrs != null) ? condAttrs.size() : 0;
				
				// Check all the comparision conditions;
				for(int i = 0; i < size; i++) {
					
					// the data types are required for the comparison;
					String condAttr = condAttrs.get(i);
					String condValue = condValues.get(i);
					String operator = operators.get(i);
					
					// Get the data type of the conditioned column;
					String realData = map.get(condAttr);
					int columnIndex = columnNameList.indexOf(condAttr);
					String dataType = dataTypeList.get(columnIndex);
					
					// column value to compare with;
					switch (operator) {
					case "=":
						// String comparison
						if (dataType.equalsIgnoreCase("text")) {
							if (!condValue.equalsIgnoreCase(realData)) {
								isValid = false;
							}
						} else {
							if (Integer.parseInt(condValue, 16) != Integer.parseInt(realData, 16)) {
								isValid = false;
							}
						}
						break;
					case "!=":
						if (dataType.equalsIgnoreCase("text")) {
							if (condValue.equalsIgnoreCase(realData)) {
								isValid = false;
							}
						} else {
							if (Integer.parseInt(condValue, 16) == Integer.parseInt(realData, 16)) {
								isValid = false;
							}
						}
						break;
					case ">":
						if (Integer.parseInt(condValue, 16) >= Integer.parseInt(realData, 16)) {
							isValid = false;
						}
						break;
					case "<":
						if (Integer.parseInt(condValue, 16) <= Integer.parseInt(realData, 16)) {
							isValid = false;
						}
						break;
					case ">=":
						if (Integer.parseInt(condValue, 16) > Integer.parseInt(realData, 16)) {
							isValid = false;
						}
						break;
					case "<=":
						if (Integer.parseInt(condValue, 16) < Integer.parseInt(realData, 16)) {
							isValid = false;
						}
						break;
					default:
						break;
					}
					
					if (!isValid) {
						break;
					}
				}
				
				if (isValid) { // the record is valid;
					
					// display the projected columns
					// 1. display the headers
					if (index == 0) {
						System.out.print("|");
						for(String columnName : projAttrs) {
							System.out.print(String.format("%24s", columnName));
						}
						System.out.print("|");
						printDoubleLines(projAttrs.size());
					}
					
					
					// 2. iterate to display all the projected columns;
					System.out.print("|");
					for(String projColumnName : projAttrs) {
						if(map.get(projColumnName).equalsIgnoreCase("NULL")){
							System.out.print(String.format("%24s", "NULL"));
							continue;
						}
						
						int columnIndex = columnNameList.indexOf(projColumnName);
						String dataType = dataTypeList.get(columnIndex);
						
						StringBuilder dateString = new StringBuilder();
						switch (dataType) {
						case "DATE":
							long dateLong = Long.parseLong(map.get(projColumnName));
							Date date = new Date(dateLong);
							
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							String strDate = dateFormat.format(date);
							System.out.print(String.format("%24s", strDate));
							break;
						case "TIME":
							int timeInt = Integer.parseInt(map.get(projColumnName));
							timeInt /= 1000;
							dateString.append(timeInt / 3600).append(":").append((timeInt % 3600) / 60).append(":").append(timeInt % 60);
							System.out.print(String.format("%24s",dateString));
							break;
						case "DATETIME":
							long dateTimeLong = Long.parseLong(map.get(projColumnName));
							Date dateTimeDate = new Date(dateTimeLong);
							DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
							String strDateTime = dateTimeFormat.format(dateTimeDate);
							System.out.print(String.format("%15s", strDateTime));
							break;
						default:
							System.out.print(String.format("%24s", map.get(projColumnName)));
							break;
						}
					}
					System.out.print("|");
					printSingleLines(projAttrs.size());
					index++;
					
				}else {
					continue;
				}
				
			}
			
		}
		
	}
	
	public static void printDoubleLines(int width) {
		System.out.println();
		for(int i = 0;  i < width * 12 ; i++) {
			System.out.print("==");
		}
		System.out.println();
	}
	public static void printSingleLines(int width) {
		System.out.println();
		for(int i = 0;  i < width * 12 ; i++) {
			System.out.print("--");
		}
		System.out.println();
	}
}
