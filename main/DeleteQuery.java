package Scralet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CLASS DeleteQuery (public, main)
 * 
 * @Function 
 * 		Do the delete command.
 * 
 * @Note 
 * 		This is a utility class, no constructor implement.
 * 		Depends on format of davisbase_table and davisbase_columns.
 * 
 * @author Zailin Yuan
 */
public class DeleteQuery 
{
	private static final int record_pointer_array_offset = 16; 			// Head address of record pointer array (first page).
	private static final int number_of_records_offset = 0x02;			// Head address of number of records in page header.
	private static final int page_number_offset = 0x06; 					// Head address of pointer to the next sibling page if overflow (first page).
	private static final int rightmost_child = 0xFFFFFFFF;						// Page number of right most leaf page.
	private static final int page_size = 512;									// Size of each file page.
	private static final String meta_column = "data/davisbase_columns.tbl"; 	// Go to meta columns to search for column positions.
	private static final String meta_table = "data/davisbase_tables.tbl"; 		// Go to meta tables to search for column positions.
	
	
	/**
	 * @Method: public static void parseDeleteCommand(String deleteString)
	 * 
	 * @Function: 
	 * 	This method's duty is to parse the command and do relative actions by calling other methods.
	 * 	This method behaves like a commander, in charge of whole deletion processes.
	 * 	This method
	 * 
	 * 	Actions: parsing command --> If command make since: search in davisbase_tables && database directory --> 
	 * 		If table exists: search in davisbase_columns --> If columns exists: search in tables we interested in 
	 * 		--> Compare with respect to conditions in where clause --> Modify pages --> return.
	 *  
	 * @param
	 * 		deleteString: delete command trimmed with all char in lower case.
	 */
	public static void parseDeleteCommand(String deleteString) 
	{
	/* check if davisbase_columns & davisbase_table are good: */
		
		File meta1 = new File(meta_table);
		File meta2 = new File(meta_column);
		
		if(!meta1.exists() || !meta2.exists())
		{
			System.out.println("Warning! " + "Meta-data for this database not found! " + "Your database may damaged!");
			return;
			// TODO: This may need a Exception!
		}
	
	/* Parse command */
		
		// Scan command words into String[]:
		Matcher m = Pattern.compile("[a-zA-Z0-9_]+|[<>=!]+|'(?:.*?)'").matcher(deleteString);
		
		ArrayList<String> commandToken = new ArrayList<>();
		while(m.find())
		{
			commandToken.add(m.group());
		}
		
	/* Check if the command is valid: */	
		
		// Command should be at least 4 words: 'delete from table DOGS'. Can't be shorter. 
		if(commandToken.size() < 4) 
		{
			System.out.println("Error: " + "Command is too short!");
			return;
		}
		
		// Typing error? second and third word must be 'from' and 'table'.
		if(!commandToken.get(1).equals("from") || !commandToken.get(2).equals("table")) 
		{
			System.out.println("Typing error: " + "Check your command again!");
			return;
		}
		
		// Just 4 words in command: Delete all records from a table: 
		if(commandToken.size() == 4) 
		{		
			System.out.println("I will delete all record from this table! " + commandToken.get(3));
			return;
		}
		
		// There is a 'where': Delete records with condition:
		// Check if the where clause is valid or not at first:
		if(!commandToken.get(4).equals("where")) 
		{
			System.out.println("Typing error: " + "Check your command 'where' clause");
		}
		else {
			if(commandToken.size() < 8)
			{
				System.out.println("Typing error: " + "Check your command 'where' clause");
				return;
			}
			
//			/** Just for testing and debugging */
//			for(String s: commandToken) 
//			{
//				System.out.println(s);
//			}
		}
		
	/* check if the where clause is valid. */
		
		// TODO: If AND / OR include in command, more implementation needed!
		if(!commandToken.get(6).matches("[<>=]|[<>!]="))
		{
			System.out.println("Typing error: " + "Valid chars for compare are '>','<','=','>=','<=','!='. "
					+ "Check your where clause!");
			return;
		}
		
	/* check if the table exist: */
		
		// First, is there such a table exist on disk in the directory?
		String table_name = commandToken.get(3); // Get the table of which records we want to delete.
		
		if(!tableExist(table_name)) 
			return;
		
		// Just for test:
		ArrayList<String> columns = new ArrayList<>();
		columns.add(commandToken.get(5));
		
		for(int i=0; i<columns.size(); ++i)
		{
			// System.out.println(columns.get(i));
		}
		
	/* 
	 * check if columns we want exist in the table we want delete from. 
	 * If yes, return all necessary Info on these columns 
	 */
		ArrayList<Pair> columnInfo = columnSearch(table_name, columns);
	}

	
	/**
	 * @Method: private static boolean tableExist(String table_name)
	 * 
	 * @Function: 
	 * 		Used to determine if the table is recorded in the davisbase_tables:	
	 * 
	 * @param 
	 * 		tableName: The table whose records we want to delete.
	 * 
	 * @return 
	 * 		true if the table is recorded in davisbase_table. Else return false.
	 */
	private static boolean tableExist(String table_name)
	{		
		String file_name = "data/" + table_name + ".tbl";
		File file = new File(file_name);
		
		// Check if the table exist on disk:
		boolean tableExistOnDisk = false; 			// flag: table on disk?
		boolean tableExistInRecord = false; 			// flag: table recorded in davisbase_tables?
		
		if(file.exists())
		{
			tableExistOnDisk = true;
		}
		
		// Then check if the table is recorded in meta-data:
		try {
			RandomAccessFile rf = new RandomAccessFile(meta_table,"r");
			rf.seek(record_pointer_array_offset); // Point to offset 16.
			
			// Search if table_name is in the davisbase_tables:
			short recordHead = rf.readShort();
			long pointer = rf.getFilePointer(); 					// Point to offset 18.
			while(recordHead != 0)
			{
				rf.seek(recordHead + 7); 							// Point to table_name type byte.
				byte tableNameType = rf.readByte(); 				// Point to head address of table name string.
				byte tableNameLen = (byte) (tableNameType - 0x0C); 	// Length of table name string.
				byte[] tableBuf = new byte[512];
				rf.read(tableBuf, 0, tableNameLen);
				
				// Is this table name the same as we want to find?
									// System.out.println(new String(tableBuf).trim());
				if(new String(tableBuf).trim().equals(table_name))
				{
					tableExistInRecord = true;
					break;
				}
				
				// Iteration update:
				rf.seek((int)pointer);
				recordHead = rf.readShort(); 	// Read from offset 18, 20, 22, ...
				pointer = rf.getFilePointer(); 	// Point to 20, 22, ...
			}
			
			// close accessor:
			rf.close();
			
			// Results display:
			if(!tableExistOnDisk && tableExistInRecord)
			{
				System.out.println("File error: " + "davisbase_table has a table in record but the table is not on disk!");
			}
			else if(tableExistOnDisk && !tableExistInRecord)
			{
				System.out.println("File error: " + "The table is on disk but not in davisbase_table's record!");
			}
			else if(!tableExistOnDisk && !tableExistInRecord)
			{
				System.out.println("Table not found!");
			}
			else
			{
				System.out.println("Table found, march to delete!");
			}			
		} catch (FileNotFoundException e) {
			System.out.println("File error: " + "davisbase_tables.tbl not found!");
			e.printStackTrace();
			return false;				// Error happens, return false to indicate no file access anymore.
		} catch (IOException e) {
			System.out.println("I/O error: " + "when trying to accessing davisbase_tables.tbl");
			e.printStackTrace();
			return false;				// Error happens, return false to indicate no file access anymore.
		}
		
		// Return whether the table exist:
		return tableExistOnDisk && tableExistInRecord;
	}
	
	
	/**
	 * @Method: private static ArrayList<Pair> columnSearch(String table_name, ArrayList<String> column_names)
	 * 
	 * @param 
	 * 		table_name: table which we want to search.
	 * @param 
	 * 		column_names: column names appeared in 'where' clause. 
	 * @return 
	 * 		An array of Pairs contains each columns' type and it's ordinal position. 
	 * 		If no such columns, return null.
	 * 
	 * 		Returned info will be used for traversing columns in user tables in the future.
	 * 
	 * @Dependance davisbase_columns format. If that changes, this method must be modified.
	 */
	private static ArrayList<Pair> columnSearch(String table_name, ArrayList<String> column_names)
	{
		// Create containers returning columns' types and ordinal_pos.
		ArrayList<Pair> columnInfo = new ArrayList<>();
		
	/* Find out if the column exist. If it does, get the column data type for future comparison. */
		try {
			// File accessor:
			RandomAccessFile metaCol = new RandomAccessFile(meta_column,"r");
			
			// Variables to update every do loop:
			int pageNum = 0;		// Used to jump out of do loop.
			long pagePos = 0;		// offset of each page.
			metaCol.seek(pagePos + number_of_records_offset);			// Used to jump out of for loop. Point to number of records in first page.
			short numOfRecords = metaCol.readShort();					// Get number of records in first page.
			long recordPtrArr = pagePos + record_pointer_array_offset;	// Head address of record pointer array in each page header.	
			
			
		/* Traversing davisbase_columns for column info: */
			// Traverse pages:
			do
			{				
				// Search records now:
				metaCol.seek(recordPtrArr); 							// Point to 16 now.
				
				// Traverse records in each page:
				long recordHead = metaCol.readShort() + (pageNum * page_size); 	// Pointer to the record to dealing with.
				long pointer = metaCol.getFilePointer(); 				// Store pointer to the next record.
				while(numOfRecords > 0)
				{
					// Get into record header for:
					// This part depends on the davisbase_column format.
					metaCol.seek(recordHead + 6); 						// start of record header, number of columns.
					byte numOfCol = metaCol.readByte(); 				// Get number of columns.
					byte tableNameType = metaCol.readByte(); 			// Get table name type.
					byte tableNameLen = (byte) (tableNameType - 0x0C);  // Get table name string length.
					byte colType = metaCol.readByte(); 					// Get column name type.
					byte colTypeLen = (byte) (colType - 0x0C); 			// Get column name length.
					byte dataType = metaCol.readByte(); 				// Get column data type.
					byte dataTypeLen = (byte) (dataType - 0x0C);		// Get column data type length.
					
					// Go to the start of record body to read table_name in meta-data:
					metaCol.seek(recordHead + 6 + numOfCol + 1);		// skip payload, rowid and record header in a record.
					byte[] tableBuf = new byte[512];
					metaCol.read(tableBuf, 0, tableNameLen); 			// Read table_name string.
					
					// Check if it is the table we're trying to find:
					System.out.println(new String(tableBuf).trim());
					if(new String(tableBuf).trim().equals(table_name))
					{
						byte[] colBuf = new byte[512];
						metaCol.read(colBuf, 0, colTypeLen);
						
						// Table is what we want, but does it have all the column we trying to find?
						String colOfTable = new String(colBuf).trim();   // Columns recorded in davisbase_columns.tbl
						System.out.println(colOfTable);
						for(int i=0; i<column_names.size(); ++i)
						{
							// The column is what we want? If yes, put it into columnInfo:
							if(column_names.get(i).equals(colOfTable))
							{
								byte[] dataTypeBuf = new byte[256];
								metaCol.read(dataTypeBuf,0,dataTypeLen);
								
								columnInfo.add(new Pair(new String(dataTypeBuf).trim(),metaCol.readByte()));
							}
						}
					}
					
					// restore record pointer:
					metaCol.seek(pointer);
					recordHead = metaCol.readShort() + (pageNum * page_size);	// Update recordHead.
					pointer = metaCol.getFilePointer();					// Update pointer.
					--numOfRecords;										// Update numOfRecords in this page.
				}
				
				// Get page number of next page:
				metaCol.seek(pagePos + page_number_offset); 			
				pageNum = metaCol.readInt();							// New pageNum. 
				
				// Get page offset of next page:
				pagePos += (pageNum * page_size);					// New pagePos.
				
				// Get number of records of next page:
				metaCol.seek(pagePos + number_of_records_offset);		
				numOfRecords = metaCol.readShort();						// New numOfRecords.
				
				// Get head address of record pointer array of next page:
				recordPtrArr = pagePos + record_pointer_array_offset;	// New recordPtrArr.
				
				System.out.println("Page number - " + pageNum + " Page position - " + pagePos + " # records - " + numOfRecords + " RPA - " + recordPtrArr);
				
			} while(pageNum != rightmost_child); 				// Reach to the rightmost page, done searching table.	
			
			// close accessor:
			metaCol.close();
			
		} catch (FileNotFoundException e) {
			System.out.println("File error: " + "Can not find " + table_name + ".tbl.");
			e.printStackTrace();
			return null;			// Error happens, return null to indicate no file access anymore.
		} catch (IOException e) {
			System.out.println("I/O error: " + "Error when accessing " + table_name + ".tbl.");
			e.printStackTrace();
			return null;			// Error happens, return null to indicate no file access anymore.
		}
		
		// Determine if all columns specified in where clause exist:
		if(columnInfo.size() == column_names.size())
		{
			return columnInfo;
		}
		else
		{
			return null;  			// Return null if any column in column_names not found in davisbase_columns.
		}
	}
	
	
} // End of class DeleteQuery.



/**
 *  CLASS: Pair
 *  
 * @Function
 * 		This is a utility for the convenience of storing two different types of values in one arrayList.
 * 		It will only be used in this package. 
 * 		Actually, it's used to store pairs like <String column_type, int ordinal_pos>.
 * 
 * @author Zailin Yuan
 *
 */
class Pair
{
	private String column_type;
	private byte ordinal_pos;
	
	// Constructor:
	public Pair(String column_type, byte ordinal_pos)
	{
		this.column_type = column_type;
		this.ordinal_pos = ordinal_pos;
	}
	
	// Get first:
	public String getColType()
	{
		return column_type;
	}
	
	// Get second:
	public byte getOrdinalPos()
	{
		return ordinal_pos;
	}
	
	// Print out:
	public String toString()
	{
		return "<" + column_type + "," + ordinal_pos +">";
	}
}

