package Scralet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.time.LocalTime;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ScarletBaseEngine {

	public static final String DB_TABLES = "davisbase_tables";
	public static final String DB_COLS = "davisbase_columns";
	public static final String DB_DIREC = "data";
	public static final int PAGE_SIZE = 512;
	public static final int LEAF = 0X0D;
	public static final int UNUSED = 0X00;

	public static void initDB() {

		System.out.println("Initializing Davisbase...");

		File directory = new File(DB_DIREC);

		if (directory.exists()) {
			// check if meta_tables exist, if not create them

			if (!(new File(directory, DB_TABLES + ".tbl").exists())) {

				init_meta(DB_TABLES);
			}
			if (!(new File(directory, DB_COLS + ".tbl").exists())) {
				init_meta(DB_COLS);
			}
		} else {
			try {

				directory.mkdir();
				init_meta(DB_TABLES);
				init_meta(DB_COLS);

			} catch (SecurityException e) {
				System.out.println(e);
			}

		}

		System.out.println("***Initialization Completed***");

	}

	public static void init_meta(String tbl) {

		if (tbl.equalsIgnoreCase(DB_TABLES))
			try {
				RandomAccessFile metaTables = new RandomAccessFile(DB_DIREC + "/" + DB_TABLES + ".tbl", "rw");

				int records = 0x02; // davisbase_tables, davisbase_columns

				int rec1_size = 8 + DB_TABLES.length();
				int rec2_size = 8 + DB_COLS.length();

				int offset_rec1 = PAGE_SIZE - rec1_size;
				int offset_rec2 = offset_rec1 - rec2_size;

				metaTables.setLength(PAGE_SIZE);
				metaTables.seek(0);
				metaTables.writeByte(LEAF); // pageType
				metaTables.writeByte(UNUSED); // 1 byte is unused.
				metaTables.writeShort(records); // number of records
				metaTables.writeShort(offset_rec2); // start of content area (after insertion of 2 records)
				metaTables.writeInt(0xffffffff);
				metaTables.writeInt(0xffffffff);
				metaTables.writeShort(UNUSED);
				metaTables.writeShort(offset_rec1);
				metaTables.writeShort(offset_rec2);

				metaTables.seek(offset_rec1);
				metaTables.writeShort(18);
				metaTables.writeInt(1);
				metaTables.writeByte(1);
				metaTables.writeByte(0x1c);
				metaTables.writeBytes(DB_TABLES);

				metaTables.seek(offset_rec2);
				metaTables.writeShort(19);
				metaTables.writeInt(2);
				metaTables.writeByte(1);
				metaTables.writeByte(0x1d);
				metaTables.writeBytes(DB_COLS);

				metaTables.close();
			} catch (Exception e) {
				System.out.println(e);
			}
		else if (tbl.equalsIgnoreCase(DB_COLS))
			try {
				RandomAccessFile metaCols = new RandomAccessFile(DB_DIREC + "/" + DB_COLS + ".tbl", "rw");

				int records = 0x09;

				int[] offset = new int[9];
				offset[0] = PAGE_SIZE - 43;
				offset[1] = offset[0] - 49;
				offset[2] = offset[1] - 44;
				offset[3] = offset[2] - 49;
				offset[4] = offset[3] - 50;
				offset[5] = offset[4] - 48;
				offset[6] = offset[5] - 58;
				offset[7] = offset[6] - 50;
				offset[8] = offset[7] - 45;

				metaCols.setLength(PAGE_SIZE);
				metaCols.seek(0);
				metaCols.writeByte(LEAF); // pageType
				metaCols.writeByte(UNUSED); // 1 byte is unused.
				metaCols.writeShort(records); // number of records
				metaCols.writeShort(offset[8]); // start of content area (after insertion of 8 records)
				metaCols.writeInt(0xffffffff);
				metaCols.writeInt(0xffffffff);
				metaCols.writeShort(UNUSED);
				for (int i = 0; i < 9; i++) {
					metaCols.writeShort(offset[i]);
				}

				metaCols.seek(offset[0]);
				metaCols.writeShort(37);
				metaCols.writeInt(1);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 16);
				metaCols.writeByte(0x0c + 5);
				metaCols.writeByte(0x0c + 3);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 3);
				metaCols.writeBytes(DB_TABLES);
				metaCols.writeBytes("rowid");
				metaCols.writeBytes("INT");
				metaCols.writeByte(1);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("YES");

				metaCols.seek(offset[1]);
				metaCols.writeShort(43);
				metaCols.writeInt(2);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 16);
				metaCols.writeByte(0x0c + 10);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 3);
				metaCols.writeBytes(DB_TABLES);
				metaCols.writeBytes("table_name");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(2);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("YES");

				metaCols.seek(offset[2]);
				metaCols.writeShort(38);
				metaCols.writeInt(3);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 5);
				metaCols.writeByte(0x0c + 3);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 3);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("rowid");
				metaCols.writeBytes("INT");
				metaCols.writeByte(1);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("YES");

				metaCols.seek(offset[3]);
				metaCols.writeShort(43);
				metaCols.writeInt(4);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 10);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("table_name");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(2);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("NO");

				metaCols.seek(offset[4]);
				metaCols.writeShort(44);
				metaCols.writeInt(5);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 11);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("column_name");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(3);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("NO");

				metaCols.seek(offset[5]);
				metaCols.writeShort(42);
				metaCols.writeInt(6);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 9);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("data_type");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(4);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("NO");

				metaCols.seek(offset[6]);
				metaCols.writeShort(52);
				metaCols.writeInt(7);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 16);
				metaCols.writeByte(0x0c + 7);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("ordinal_position");
				metaCols.writeBytes("TINYINT");
				metaCols.writeByte(5);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("NO");

				metaCols.seek(offset[7]);
				metaCols.writeShort(44);
				metaCols.writeInt(8);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 11);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("is_nullable");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(6);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("NO");

				metaCols.seek(offset[8]);
				metaCols.writeShort(38);
				metaCols.writeInt(9);
				metaCols.writeByte(6);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 6);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("unique");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(7);
				metaCols.writeBytes("NO");
				metaCols.writeBytes("NO");

				metaCols.close();
			} catch (Exception e) {
				System.out.println(e);
			}
	}

	public static void parseCreateTable(String createTableString) {

		String validFormat = "CREATE TABLE <table_name> (<col_name> <data_type> [NOT_NULL] [UNIQUE]);";

		createTableString = createTableString.replaceAll("\\s+", " ");

		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		if (!createTableTokens.get(1).equalsIgnoreCase("table")) {
			System.out.println("Invalid format! Valid format is ");
			System.out.println(validFormat);
		} else {
			if((!createTableTokens.contains("\\("))||(createTableTokens.contains("\\("))){
				System.out.println("Invalid format! Valid format is ");
				System.out.println(validFormat);
				return;
			}

			String table_name = createTableTokens.get(2);

			String[] temp = createTableString.replaceAll("\\(", " ").replaceAll("\\)", " ").split(table_name);

			String[] cols = temp[1].trim().split(",");

			ArrayList<String> columns = new ArrayList<String>();
			ArrayList<String> dataTypes = new ArrayList<String>();
			ArrayList<Boolean> is_nullable = new ArrayList<Boolean>();
			ArrayList<Boolean> unique = new ArrayList<Boolean>();

			columns.add("rowid");
			dataTypes.add("INT");
			is_nullable.add(false);
			unique.add(true);

			for (int i = 1; i < cols.length + 1; i++) {

				ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(cols[i - 1].trim().split(" ")));

				if (tokens.size() < 2) {
					System.out.println("Invalid Format! Valid format is");
					System.out.println(validFormat);
					return;
				} else {
					columns.add(tokens.get(0).trim()); // column_name
					dataTypes.add(tokens.get(1).trim().toUpperCase()); // dataTypes

					if (tokens.contains("not") && tokens.contains("null")) {
						is_nullable.add(false);
					} else {
						is_nullable.add(true);
					}

					if (tokens.contains("unique")) {

						unique.add(true);
					} else {
						unique.add(false);
					}

				}
			}

			try {
				File directory = new File(DB_DIREC);

				if ((new File(directory, table_name + ".tbl").exists())) {
					System.out.println("Table already exists");
					return;
				}
				RandomAccessFile tableFile = new RandomAccessFile(DB_DIREC + "/" + table_name + ".tbl", "rw");
				tableFile.setLength(PAGE_SIZE);
				tableFile.seek(0);
				tableFile.writeByte(LEAF); // pageType
				tableFile.skipBytes(5);
				tableFile.writeInt(0xffffffff);
				tableFile.writeInt(0xffffffff);
				tableFile.close();

			} catch (Exception e) {
				System.out.println(e);
			}

			Table table = new Table();
			table.setTbl_name(table_name);
			table.setNum_cols(cols.length + 1);
			table.setCols(columns);
			table.setDataTypes(dataTypes);
			table.setIs_nullable(is_nullable);
			table.setUnique(unique);

			insert_into_meta(table, DB_TABLES);
			insert_into_meta(table, DB_COLS);

		}

	}

	public static void insert_into_meta(Table table, String tbl_name) {

		try {
			RandomAccessFile file = new RandomAccessFile(DB_DIREC + "/" + tbl_name + ".tbl", "rw");

			// check which page to insert
			int page = Page.get_freePage(file);

			int num_recs = Page.get_numRecs(file, page);

			int conStart = Page.get_conStart(file, page);

			int row_id = Page.get_rowID(file, page);

			if (tbl_name.equalsIgnoreCase(DB_TABLES)) {

				int rec_size = 8 + table.getTbl_name().length();

				if (Page.check_overflow(file, page, rec_size)) {

					page = Page.createLeafPage(file, page);
					num_recs = 0;
					conStart = 512;

				}

				int offset_rec = conStart - rec_size;
				file.seek(page * PAGE_SIZE + 2);
				file.writeShort(num_recs + 1); // number of records
				file.writeShort(offset_rec); // start of content area (after insertion of records)
				file.seek(16 + (num_recs * 2));
				file.writeShort(offset_rec);

				file.seek(page * PAGE_SIZE + offset_rec);
				file.writeShort(2 + table.getTbl_name().length());
				file.writeInt(row_id + 1);
				file.writeByte(1);
				file.writeByte(0x0c + table.getTbl_name().length());
				file.writeBytes(table.getTbl_name());

			} else {

				int recs_to_insert = table.getNum_cols();

				for (int i = 0; i < recs_to_insert; i++) {
					int len = table.getTbl_name().length() + table.getCols().get(i).length()
							+ table.getDataTypes().get(i).length();
					int consLen1 = table.is_nullable.get(i) ? 3 : 2;
					int consLen2 = table.unique.get(i) ? 3 : 2;
					int rec_size = 14 + len + consLen1+consLen2;
					if (Page.check_overflow(file, page, rec_size)) {

						page = Page.createLeafPage(file, page);
						num_recs = 0;
						conStart = 512;

					}
					int offset_rec = conStart - rec_size;
					file.seek(page * PAGE_SIZE + 2);
					file.writeShort(num_recs + 1); // number of records
					file.writeShort(offset_rec); // start of content area (after insertion of records)
					file.skipBytes(10 + (num_recs * 2));

					file.writeShort(offset_rec);

					file.seek(page * PAGE_SIZE + offset_rec);

					file.writeShort(8 + len + consLen1+consLen2);
					file.writeInt(row_id + 1);
					file.writeByte(6);
					file.writeByte(0x0c + table.getTbl_name().length());
					file.writeByte(0x0c + table.getCols().get(i).length());
					file.writeByte(0x0c + table.getDataTypes().get(i).length());
					file.writeByte(1);
					file.writeByte(0x0c + consLen1);
					file.writeByte(0x0c + consLen2);
					file.writeBytes(table.getTbl_name());
					file.writeBytes(table.getCols().get(i));
					file.writeBytes(table.getDataTypes().get(i));
					file.writeByte(i + 1);
					file.writeBytes(table.is_nullable.get(i) ? "YES" : "NO");
					file.writeBytes(table.unique.get(i) ? "YES" : "NO");

					conStart = offset_rec;
					num_recs++;
					row_id++;

				}
			}

			file.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static Table GetColumnTable(String filename) {
		int PAGE_SIZE = 512;
		int INT_OFFSET = 4;
		int SHORT_OFFSET = 2;
		int BOOL_OFFSET = 1;
		int BYTE_OFFSET = 1;
		int CHAR_OFFSET = 1;
		int rec_arr_offset = 0x10;
		byte LEAF = 0x0d;

		Table col_tbl = new Table();
		ArrayList<Integer> rowids = new ArrayList<Integer>();
		ArrayList<String> tbls = new ArrayList<String>();
		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<String> dataTypes = new ArrayList<String>();
		ArrayList<Byte> ord_pos = new ArrayList<Byte>();
		ArrayList<Boolean> is_nullable = new ArrayList<Boolean>();
		ArrayList<Boolean> unique = new ArrayList<Boolean>();
		int page_offset = 0;

		try {
			RandomAccessFile colfile = new RandomAccessFile(filename, "rw");
			// read the page header
			colfile.seek(page_offset + 0x06); // right sibling offset
			int right_sibling_flag = colfile.readInt();
			while (true) {
				colfile.seek(page_offset + 0x02);
				short rec_count = colfile.readShort();// number of records in this page
				short last_rec_begins = colfile.readShort();// the beginning of the last record
				ArrayList<Short> rec_offsets = new ArrayList<Short>();
				colfile.seek(page_offset + rec_arr_offset);
				for (short i = 0; i < rec_count; i++) {
					rec_offsets.add(colfile.readShort());
				}
				colfile.seek(page_offset + 0x06); // update right sibling offset
				right_sibling_flag = colfile.readInt();
				colfile.seek(page_offset + last_rec_begins);
				colfile.skipBytes(2);
				int last_rowid = colfile.readInt();
				byte col_count = colfile.readByte();
				col_tbl.setNum_cols(col_count);
				for (short i : rec_offsets) {
					colfile.seek(page_offset + i);
					short payload = colfile.readShort();
					int rowid = colfile.readInt();
					rowids.add(rowid);
					byte[] col_type = new byte[col_count];
					colfile.skipBytes(1);
					for (short j = 0; j < col_count; j++) {
						col_type[j] = colfile.readByte();
					}
					String table_name = read_string(colfile, col_type[0] - 0x0c);
					tbls.add(table_name);
					String column_name = read_string(colfile, col_type[1] - 0x0c);
					cols.add(column_name);
					String data_type = read_string(colfile, col_type[2] - 0x0c);
					dataTypes.add(data_type);
					byte ordinal_position = colfile.readByte();
					ord_pos.add(ordinal_position);
					String is_null = read_string(colfile, col_type[4] - 0x0c);
					is_nullable.add(is_null.contentEquals("YES"));
					String is_unique = read_string(colfile, col_type[5] - 0x0c);
					unique.add(is_unique.contentEquals("YES"));
				}
				if (right_sibling_flag == 0xffffffff)
					break;
				else
					page_offset = right_sibling_flag * PAGE_SIZE;
			}
			col_tbl.settbls(tbls);
			col_tbl.setCols(cols);
			col_tbl.setDataTypes(dataTypes);
			col_tbl.setIs_nullable(is_nullable);
			col_tbl.setUnique(unique);
			colfile.close();
		}

		catch (Exception e) {
			System.out.println(e);
		}

		return col_tbl;
	}

	public static String read_string(RandomAccessFile raf, int length) {

		byte[] buffer = new byte[length];
		try {
			raf.read(buffer);
		} catch (Exception e) {
			System.out.println(e);
		}
		String result = new String(buffer);

		return result;
	}

	public static ArrayList<String> get_columns(Table metacol, String table_name) {
		ArrayList<String> tbls = metacol.gettbls();
		int startingIdx = 0;
		while (!tbls.get(startingIdx).equalsIgnoreCase(table_name)) {
			startingIdx++;
		}
		int endIdx = startingIdx;
		while (tbls.get(endIdx).equalsIgnoreCase(table_name)) {
			endIdx++;
			if (endIdx == tbls.size())
				break;
		}
		ArrayList<String> attributes = new ArrayList<String>();
		ArrayList<String> cols = metacol.getCols();
		for (int i = startingIdx + 1; i < endIdx; i++) {
			attributes.add(cols.get(i).toLowerCase());
		}
		return attributes;
	}

	public static ArrayList<String> get_dataTypes(Table metacol, String table_name) {
		ArrayList<String> tbls = metacol.gettbls();
		int startingIdx = 0;
		while (!tbls.get(startingIdx).equalsIgnoreCase(table_name)) {
			startingIdx++;
		}
		int endIdx = startingIdx;
		while (tbls.get(endIdx).equalsIgnoreCase(table_name)) {
			endIdx++;
			if (endIdx == tbls.size())
				break;
		}
		ArrayList<String> datatypes = metacol.getDataTypes();
		ArrayList<String> dt = new ArrayList<String>();
		for (int i = startingIdx + 1; i < endIdx; i++) {
			dt.add(datatypes.get(i).toUpperCase());
		}
		return dt;

	}

	public static ArrayList<String> get_isnullable(Table metacol, String table_name) {
		ArrayList<String> tbls = metacol.gettbls();
		int startingIdx = 0;
		while (!tbls.get(startingIdx).equalsIgnoreCase(table_name)) {
			startingIdx++;
		}
		int endIdx = startingIdx;
		while (tbls.get(endIdx).equalsIgnoreCase(table_name)) {
			endIdx++;
			if (endIdx == tbls.size())
				break;
		}
		ArrayList<Boolean> isnullable = metacol.getIs_nullable();
		ArrayList<String> inl = new ArrayList<String>();
		for (int i = startingIdx + 1; i < endIdx; i++) {
			// System.out.println("" + isnullable.get(i));
			if (isnullable.get(i) == true) {
				inl.add("yes");
			} else {
				inl.add("no");
			}
		}
		return inl;

	}

	public static void InsertOperation(String table_name, ArrayList<String> insertrecord, ArrayList<String> dataTypes) {
		String filename = "data/" + table_name + ".tbl";
		try {
			RandomAccessFile tablefile = new RandomAccessFile(filename, "rw");
			int page_offset = 0;
			int right_most_page_num = Page.get_freePage(tablefile);
			page_offset += PAGE_SIZE * right_most_page_num; // the right most child pointer is an integer of page
															// number?
			// Now the pointer is at the right most leaf, calculate the required space first

			byte rec_length = 7;
			rec_length += dataTypes.size(); // record the length of a tuple, the cell header takes 6 bytes
			byte[] Data_types = new byte[dataTypes.size()]; // The record header

			for (int index = 0; index < dataTypes.size(); index++) {
				if (insertrecord.get(index).equals("NA")) {
					Data_types[index] = 0x00;
					continue;
				}
				switch (dataTypes.get(index)) {
				case "TINYINT":
					rec_length += 1;
					Data_types[index] = 0x01;
					break;
				case "SMALLINT":
					rec_length += 2;
					Data_types[index] = 0x02;
					break;
				case "INT":
					rec_length += 4;
					Data_types[index] = 0x03;
					break;
				case "LONG":
					rec_length += 8;
					Data_types[index] = 0x04;
					break;
				case "FLOAT":
					rec_length += 4;
					Data_types[index] = 0x05;
					break;
				case "DOUBLE":
					rec_length += 8;
					Data_types[index] = 0x06;
					break;
				case "YEAR":
					rec_length += 1;
					Data_types[index] = 0x08;
					break;
				case "TIME":
					rec_length += 4;
					Data_types[index] = 0x09;
					break;
				case "DATETIME":
					rec_length += 8;
					Data_types[index] = 0x0A;
					break;
				case "DATE":
					rec_length += 8;
					Data_types[index] = 0x0B;
					break;
				case "TEXT":
					rec_length += insertrecord.get(index).length();
					Data_types[index] = 0x0c;
					Data_types[index] += insertrecord.get(index).length();
					break;
				default:
					System.out.println("Error on insertion: unknown data type");
					return;
				}
			}
			// length is calculated, now check if the current page has enough space
			tablefile.seek(page_offset + 0x02);
			short rec_num = tablefile.readShort();
			short last_record = tablefile.readShort();
			int last_rowid = 0;
			if (last_record == 0)
				last_record = 512;// A newly created table without any record
			else {
				last_rowid = Page.get_rowID(tablefile, right_most_page_num);
			}
			if (last_record - rec_num * 2 - 0x10 < rec_length) {
				right_most_page_num = Page.createLeafPage(tablefile, right_most_page_num); // handle overflow, reset
																							// page offset pointer
				page_offset = PAGE_SIZE * right_most_page_num;
				rec_num = 0;
				last_record = PAGE_SIZE;
			}
			// update the page header
			tablefile.seek(page_offset + 0x02);
			rec_num += 1;
			tablefile.writeShort(rec_num);// update record counts
			last_record -= rec_length; // update last record pointer
			tablefile.writeShort(last_record);
			tablefile.seek(page_offset + 0x10 + (rec_num - 1) * 2);
			tablefile.writeShort(last_record);// append the last record pointer in the record pointer array

			// next, write the record header
			tablefile.seek(page_offset + last_record);
			tablefile.writeShort(rec_length - 2);// payload
			tablefile.writeInt(last_rowid + 1); // rowid
			tablefile.writeByte(dataTypes.size());// number of columns
			for (byte b : Data_types) { // list of column data types
				tablefile.writeByte(b);
			}
			for (int index = 0; index < dataTypes.size(); index++) {
				if (insertrecord.get(index).equals("NA"))
					continue;
				switch (dataTypes.get(index)) {
				case "TINYINT":
					short b = Short.parseShort(insertrecord.get(index));
					tablefile.writeByte(b);
					break;
				case "SMALLINT":
					short st = Short.parseShort(insertrecord.get(index));
					tablefile.writeShort(st);
					break;
				case "INT":
					int num = Integer.parseInt(insertrecord.get(index));
					tablefile.writeInt(num);
					break;
				case "LONG":
					long ln = Long.parseLong(insertrecord.get(index));
					tablefile.writeLong(ln);
					break;
				case "FLOAT":
					float fl = Float.parseFloat(insertrecord.get(index));
					tablefile.writeFloat(fl);
					break;
				case "DOUBLE":
					double db = Double.parseDouble(insertrecord.get(index));
					tablefile.writeDouble(db);
					break;
				case "YEAR":
					short y = Short.parseShort(insertrecord.get(index));
					y -= 2000;
					tablefile.writeByte(y);
					break;
				case "TIME":
					try {
						LocalTime lt = LocalTime.parse(insertrecord.get(index));
						int millis = 1000 * (lt.getHour() * 3600 + lt.getMinute() * 60 + lt.getSecond());
						tablefile.writeInt(millis);
					} catch (Exception e) {
						System.out.println("time format error, must be HH:MM:SS");
					}
					break;
				case "DATETIME":
					try {
						Date dt = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(insertrecord.get(index));
						long datetime = dt.getTime();
						tablefile.writeLong(datetime);
					} catch (Exception e) {
						System.out.println("datetime format error, must be yyyy-MM-dd_HH:mm:ss");
					}
					break;
				case "DATE":
					try {
						Date date = new SimpleDateFormat("yyyy-MM-dd").parse(insertrecord.get(index));
						long date_as_time = date.getTime();
						tablefile.writeLong(date_as_time);
					} catch (Exception e) {
						System.out.println("datetime format error, must be yyyy-MM-dd");
					}
					break;
				case "TEXT":
					tablefile.writeBytes(insertrecord.get(index));
					break;
				default:
					return;
				}
			}
			tablefile.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void parseQuery(String queryString) {
		try {
			SelectQuery.parseQueryCommand(queryString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void dropTable(String dropTableString) {

	}

	public static void parseUpdate(String updateString) {

	}

	public static void parseDelete(String deleteString) {
		DeleteQuery.parseDeleteCommand(deleteString);
	}
}
