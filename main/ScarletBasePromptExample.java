package Scralet;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ArrayDeque;
import static java.lang.System.out;
import java.time.LocalTime;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 *  @author Chris Irwin Davis
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started wiht read/write of
 *     binary data files using RandomAccessFile class</p>
 *  </b>
 *
 */
public class ScarletBasePromptExample {

	/* This can be changed to whatever you like */
	static String prompt = "scarletsql> ";
	static String version = "v0.6";
	static String copyright = "@2019 Team Red";
	static boolean isExit = false;
	/*
	 * Page size for all files is 512 bytes by default.
	 * You may choose to make it user modifiable
	 */
	static long pageSize = 512; 

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();
		ScarletBaseEngine.initDB();
		/* Variable to collect user input from the prompt */
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}

	/** ***********************************************************************
	 *  Static method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to ScarletBaseLite"); // Display the string.
		System.out.println("ScarletBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			out.println(line("*",80));
			out.println("SUPPORTED COMMANDS\n");
			out.println("All commands below are case insensitive\n");
			out.println("SHOW TABLES;");
			out.println("\tDisplay the names of all tables.\n");
			//printCmd("SELECT * FROM <table_name>;");
			//printDef("Display all records in the table <table_name>.");
			out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
			out.println("\tDisplay table records whose optional <condition>");
			out.println("\tis <column_name> = <value>.\n");
			out.println("DROP TABLE <table_name>;");
			out.println("\tRemove table data (i.e. all records) and its schema.\n");
			out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
			out.println("\tModify records data whose optional <condition> is\n");
			out.println("VERSION;");
			out.println("\tDisplay the program version.\n");
			out.println("HELP;");
			out.println("\tDisplay this help information.\n");
			out.println("EXIT;");
			out.println("\tExit the program.\n");
			out.println(line("*",80));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("ScarletBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		

		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "show":
				System.out.println("CASE: SHOW TABLES");
				parseQuery(userCommand);
				break;
			case "select":
				System.out.println("CASE: SELECT");
				try {
					parseQuery(userCommand);
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;
			case "drop":
				System.out.println("CASE: DROP");
				dropTable(userCommand);
				break;
			case "create":
				System.out.println("CASE: CREATE");
				parseCreateTable(userCommand);
				break;
			case "insert":
				System.out.println("CASE: INSERT");
				parseInsert(userCommand);
				break;
			case "update":
				System.out.println("CASE: UPDATE");
				parseUpdate(userCommand);
				break;
			case "delete":
				System.out.println("CASE: DELETE");
				parseDelete(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
	
	public static void parseDelete(String deleteString) {
		System.out.println("STUB: This is the parseDelete method.");
		System.out.println("\tParsing the string:\"" + deleteString + "\"");
		ScarletBaseEngine.parseDelete(deleteString);
	}
	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: This is the dropTable method.");
		System.out.println("\tParsing the string:\"" + dropTableString + "\"");
	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 */
	public static void parseQuery(String queryString) {
		System.out.println("STUB: This is the parseQuery method");
		System.out.println("\tParsing the string:\"" + queryString + "\"");
		ScarletBaseEngine.parseQuery(queryString);
	}

	/**
	 *  Stub method for updating records
	 *  @param updateString is a String of the user input
	 */
	public static void parseUpdate(String updateString) {
		System.out.println("STUB: This is the dropTable method");
		System.out.println("Parsing the string:\"" + updateString + "\"");
	}
	public static void parseInsert(String insertString) {
		//System.out.println("STUB: This is the insert method");
		//System.out.println("Parsing the string:\"" + insertString + "\"");
		//first, parse the input command into tokens
		//insertString = insertString.toLowerCase();
		insertString = insertString.replaceAll(",\\s+", ",");
		//System.out.println(insertString);
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(insertString.split(" ")));
		String ValidFormat = "INSERT INTO table_name (column_list) VALUES (value_list);";
		ArrayList<String> insertrecord = new ArrayList<String>();
		if(commandTokens.size()!= 6&&commandTokens.size()!=5){
			System.out.println("Syntax error on insert.The right format is: ");
			System.out.println(ValidFormat);
			return;
		}
		//second, check if the format satisfies the format
		if(commandTokens.size()== 6){
			if (!commandTokens.get(1).equalsIgnoreCase("into") || !commandTokens.get(4).equalsIgnoreCase("values") || !commandTokens.get(3).startsWith("(") || !commandTokens.get(3).endsWith(")") || !commandTokens.get(5).startsWith("(") || !commandTokens.get(5).endsWith(")")){
				System.out.println("Syntax error on insert. The right format is:" + ValidFormat);
				return;
				}
				}
			else{
					if (!commandTokens.get(1).equalsIgnoreCase("into") || !commandTokens.get(3).equalsIgnoreCase("values") || !commandTokens.get(4).startsWith("(") || !commandTokens.get(4).endsWith(")")){
	                        System.out.println("Syntax error on insert. The right format is:" + ValidFormat);
	                        return;}
			}
		//next, get the table name, to search the table meta-data
		String table_name = commandTokens.get(2);
		
		File directory = new File("data");

		if (!(new File(directory, table_name + ".tbl").exists())) {
			System.out.println("Table doesn't exist!");
			return;
		}
		//read the column names and data types from the column meta-data
		//get the indices of the columns first
		Table col_tbl = ScarletBaseEngine.GetColumnTable("data/davisbase_columns.tbl");
		ArrayList<String> attributes = ScarletBaseEngine.get_columns(col_tbl, table_name.toLowerCase());
	//	System.out.println("attributes loaded with size " + attributes.size());
		
		ArrayList<String> data_types = ScarletBaseEngine.get_dataTypes(col_tbl, table_name.toLowerCase());

		ArrayList<String> is_nullable = ScarletBaseEngine.get_isnullable(col_tbl, table_name.toLowerCase());
		//further parse the command tokens to get the columns and values to be inserted
		
		if(commandTokens.size() == 5){
			ArrayList<String> insertcolumns = attributes;
			ArrayList<String> queryrecords = new ArrayList<String>(Arrays.asList(commandTokens.get(4).substring(1,(commandTokens.get(4).length()-1)).split(",")));
			for(String s : insertcolumns){
				s = s.trim();
			}	
			for(String s : queryrecords){
	                        s = s.trim();
	                }
			if(insertcolumns.size()!=queryrecords.size()) {
	                        System.out.println("Syntax error on insert, the columns and values have to match!");
	                        return;
	                } 
			insertrecord = queryrecords;
			}
			else{
			ArrayList<String> insertcolumns = new ArrayList<String>(Arrays.asList(commandTokens.get(3).substring(1,(commandTokens.get(3).length()-1)).split(",")));
			ArrayList<String> queryrecords = new ArrayList<String>(Arrays.asList(commandTokens.get(5).substring(1,(commandTokens.get(5).length()-1)).split(",")));
			for(String s : insertcolumns){
				s = s.trim();
			}
			for(String s : queryrecords){
				s = s.trim();
			}
			if(insertcolumns.size()!=queryrecords.size()) {
	                        System.out.println("Syntax error on insert, the columns and values have to match!");
	                        return;
	                }
		//Fill in the blanks if column is missing in the query
	
		
		for(int i = 0, j = 0; i<attributes.size(); i++) {
			if(j == insertcolumns.size()) {
				insertrecord.add("NA");
			}
			else {
				String tmp = insertcolumns.get(j);
				tmp = tmp.toLowerCase();
	//			System.out.println(attributes.get(i));
				if(!attributes.contains(tmp)){
		
					System.out.println("Error on insertion: column " + tmp + " does not exist." );
					return;
				}

				else if(attributes.get(i).equalsIgnoreCase(tmp)){
					insertrecord.add(queryrecords.get(j));
					j++;
				}
				else{
					insertrecord.add("NA");
				}
			}
		}
			}
		for(int i = 0; i < insertrecord.size(); i++){
			if(insertrecord.get(i).equals("NA")) {
				if(is_nullable.get(i).equals("no")) {
					System.out.println("Error on insertion: "+ attributes.get(i)+ " cannot be null!");
				
				return;}
			}
			else if (data_types.get(i).equals("TIME")){
					try{
						LocalTime lt = LocalTime.parse(insertrecord.get(i));
					}
					catch(Exception e){
						System.out.println("time format error, must be HH:MM:SS");
						return;						}
                              }
                        else if (data_types.get(i).equals("DATETIME")){
						try{
							Date dt = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(insertrecord.get(i));
						}
						catch(Exception e) {
							System.out.println("datetime format error, must be yyyy-MM-dd_HH:mm:ss");
						return;
						}
                                               }
                        else if (data_types.get(i).equals("DATE")){
						try{
                                                        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(insertrecord.get(i));
                                                }       
                                                catch(Exception e) {
                                                        System.out.println("date format error, must be yyyy-MM-dd");
							return;
                                                }
                                                }
		}
		System.out.println("parse insert success");
		ScarletBaseEngine.InsertOperation(table_name, insertrecord, data_types);		
	}
	
	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 */
	public static void parseCreateTable(String createTableString) {
		
		
		
			ScarletBaseEngine.parseCreateTable(createTableString);
		
	}
}