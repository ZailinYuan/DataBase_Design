//package edu.utdallas.davisbase;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class DavisbaseEngine {

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

			if (!(new File(directory, DB_TABLES).exists())) {

				init_meta(DB_TABLES);
			}
			if (!(new File(directory, DB_COLS).exists())) {
				init_meta(DB_COLS);
			}
		} else {
			try {

				directory.mkdir();

			} catch (SecurityException e) {
				System.out.println(e);
			}
			init_meta(DB_TABLES);
			init_meta(DB_COLS);

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

				int records = 0x08;

				int[] offset = new int[8];
				offset[0] = PAGE_SIZE - 39;
				offset[1] = offset[0] - 45;
				offset[2] = offset[1] - 40;
				offset[3] = offset[2] - 46;
				offset[4] = offset[3] - 47;
				offset[5] = offset[4] - 45;
				offset[6] = offset[5] - 55;
				offset[7] = offset[6] - 47;

				metaCols.setLength(PAGE_SIZE);
				metaCols.seek(0);
				metaCols.writeByte(LEAF); // pageType
				metaCols.writeByte(UNUSED); // 1 byte is unused.
				metaCols.writeShort(records); // number of records
				metaCols.writeShort(offset[7]); // start of content area (after insertion of 8 records)
				metaCols.writeInt(0xffffffff);
				metaCols.writeInt(0xffffffff);
				metaCols.writeShort(UNUSED);
				for (int i = 0; i < 8; i++) {
					metaCols.writeShort(offset[i]);
				}

				metaCols.seek(offset[0]);
				metaCols.writeShort(33);
				metaCols.writeInt(1);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 16);
				metaCols.writeByte(0x0c + 5);
				metaCols.writeByte(0x0c + 3);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_TABLES);
				metaCols.writeBytes("rowid");
				metaCols.writeBytes("INT");
				metaCols.writeByte(1);
				metaCols.writeBytes("NO");

				metaCols.seek(offset[1]);
				metaCols.writeShort(39);
				metaCols.writeInt(2);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 16);
				metaCols.writeByte(0x0c + 10);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_TABLES);
				metaCols.writeBytes("table_name");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(2);
				metaCols.writeBytes("NO");

				metaCols.seek(offset[2]);
				metaCols.writeShort(34);
				metaCols.writeInt(3);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 5);
				metaCols.writeByte(0x0c + 3);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("rowid");
				metaCols.writeBytes("INT");
				metaCols.writeByte(1);
				metaCols.writeBytes("NO");

				metaCols.seek(offset[3]);
				metaCols.writeShort(40);
				metaCols.writeInt(4);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 10);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("table_name");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(2);
				metaCols.writeBytes("NO");

				metaCols.seek(offset[4]);
				metaCols.writeShort(41);
				metaCols.writeInt(5);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 11);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("column_name");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(3);
				metaCols.writeBytes("NO");

				metaCols.seek(offset[5]);
				metaCols.writeShort(39);
				metaCols.writeInt(6);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 9);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("data_type");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(4);
				metaCols.writeBytes("NO");

				metaCols.seek(offset[6]);
				metaCols.writeShort(49);
				metaCols.writeInt(7);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 16);
				metaCols.writeByte(0x0c + 7);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("ordinal_position");
				metaCols.writeBytes("TINYINT");
				metaCols.writeByte(5);
				metaCols.writeBytes("NO");

				metaCols.seek(offset[7]);
				metaCols.writeShort(41);
				metaCols.writeInt(8);
				metaCols.writeByte(5);
				metaCols.writeByte(0x0c + 17);
				metaCols.writeByte(0x0c + 11);
				metaCols.writeByte(0x0c + 4);
				metaCols.writeByte(1);
				metaCols.writeByte(0x0c + 2);
				metaCols.writeBytes(DB_COLS);
				metaCols.writeBytes("is_nullable");
				metaCols.writeBytes("TEXT");
				metaCols.writeByte(6);
				metaCols.writeBytes("NO");
				metaCols.close();
			} catch (Exception e) {
				System.out.println(e);
			}
	}

	public static void parseCreateTable(String createTableString) {

		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		if (!createTableTokens.get(1).equalsIgnoreCase("table")) {
			System.out.println("Syntax Error in create Table string!");
		} else {

			String table_name = createTableTokens.get(2);

			try {
				File directory = new File(DB_DIREC);

				if ((new File(directory, table_name).exists())) {
					System.out.println("Table already exists");
					return;
				}
				RandomAccessFile tableFile = new RandomAccessFile(DB_DIREC + "/" + table_name + ".tbl", "rw");
				tableFile.setLength(PAGE_SIZE);
				tableFile.seek(0);
				tableFile.writeByte(LEAF); // pageType
				tableFile.close();

			} catch (Exception e) {
				System.out.println(e);
			}

			Table table = new Table();
			table.setTbl_name(table_name);

			String[] temp = createTableString.replaceAll("\\(", " ").replaceAll("\\)", " ").split(table_name);
			String[] cols = temp[1].trim().split(",");

			table.setNum_cols(cols.length + 1);

			ArrayList<String> columns = new ArrayList<String>();
			ArrayList<String> dataTypes = new ArrayList<String>();
			ArrayList<Boolean> is_nullable = new ArrayList<Boolean>();
			

			columns.add("rowid");
			dataTypes.add("INT");
			is_nullable.add(false);

			for (int i = 1; i < cols.length + 1; i++) {

				ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(cols[i - 1].trim().split(" ")));
				columns.add(tokens.get(0).trim()); // column_name
				dataTypes.add(tokens.get(1).trim().toUpperCase()); // dataTypes
				if (tokens.contains("not") && tokens.contains("null")) {
					is_nullable.add(false);
				} else {
					is_nullable.add(true);
				}

			}
			table.setCols(columns);
			table.setDataTypes(dataTypes);
			table.setIs_nullable(is_nullable);

			insert_into_meta(table, DB_TABLES);
			insert_into_meta(table, DB_COLS);

		}

	}

	public static void insert_into_meta(Table table, String tbl_name) {

		try {
			RandomAccessFile file = new RandomAccessFile(DB_DIREC + "/" + tbl_name + ".tbl", "rw");

			// check which page to insert
			int num_pages = (int) (file.length() / (new Long(PAGE_SIZE)));

			file.seek((num_pages - 1) * PAGE_SIZE + 2);

			int num_recs = file.readShort();
			int loc = file.readShort();

			// get the rowid in the page
			file.seek(loc + 2);
			int row_id = file.readInt();

			int cur_size = 16 + (num_recs * 2) + (PAGE_SIZE - loc);

			if (tbl_name.equalsIgnoreCase(DB_TABLES)) {

				int rec_size = 8 + table.getTbl_name().length();

				if (rec_size + 2 < (PAGE_SIZE - cur_size)) {

					int offset_rec = loc - rec_size;
					file.seek((num_pages - 1) * PAGE_SIZE + 2);
					file.writeShort(num_recs + 1); // number of records
					file.writeShort(offset_rec); // start of content area (after insertion of records)
					file.seek(16 + (num_recs * 2));

					file.writeShort(offset_rec);

					file.seek(offset_rec);
					file.writeShort(2 + table.getTbl_name().length());
					file.writeInt(row_id + 1);
					file.writeByte(1);
					file.writeByte(0x0c + table.getTbl_name().length());
					file.writeBytes(table.getTbl_name());

				}
			} else {

				int recs_to_insert = table.getNum_cols();

				for (int i = 0; i < recs_to_insert; i++) {
					int len = table.getTbl_name().length() + table.getCols().get(i).length()
							+ table.getDataTypes().get(i).length();
					int consLen = table.is_nullable.get(i) ? 3 : 2;
					int rec_size = 13 + len + consLen;
					if (rec_size + 2 < (PAGE_SIZE - cur_size)) {

						int offset_rec = loc - rec_size;
						file.seek((num_pages - 1) * PAGE_SIZE + 2);
						file.writeShort(num_recs + 1); // number of records
						file.writeShort(offset_rec); // start of content area (after insertion of records)
						file.seek(16 + (num_recs * 2));

						file.writeShort(offset_rec);

						file.seek(offset_rec);

						file.writeShort(7 + len + consLen);
						file.writeInt(row_id + 1);
						file.writeByte(5);
						file.writeByte(0x0c + table.getTbl_name().length());
						file.writeByte(0x0c + table.getCols().get(i).length());
						file.writeByte(0x0c + table.getDataTypes().get(i).length());
						file.writeByte(1);
						file.writeByte(0x0c + consLen);
						file.writeBytes(table.getTbl_name());
						file.writeBytes(table.getCols().get(i));
						file.writeBytes(table.getDataTypes().get(i));
						file.writeByte(i + 1);
						file.writeBytes(table.is_nullable.get(i) ? "YES" : "NO");
						cur_size = cur_size + rec_size;
						loc = offset_rec;
						num_recs++;
						row_id++;

					}
				}

			}

			file.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void parseQuery(String queryString) {

	}

	public static void dropTable(String dropTableString) {

	}

	public static void parseUpdate(String updateString) {

	}

	public static void parseInsert(String insertString) {

	}

	public static void parseDelete(String deleteString) {

	}
}
