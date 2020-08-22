//package edu.utdallas.davisbase;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Page {

	public static final String DB_TABLES = "davisbase_tables";
	public static final String DB_COLS = "davisbase_columns";
	public static final String DB_DIREC = "data";
	public static final int PAGE_SIZE = 512;
	public static final int LEAF = 0X0D;
	public static final int UNUSED = 0X00;
	public static final int INTERIOR = 0X05;

	public static int get_numPages(RandomAccessFile file) {

		int numPages = -1;
		try {
			numPages = (int) (file.length() / (new Long(PAGE_SIZE)));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return numPages;

	}

	public static int getPageType(RandomAccessFile file, int page_num) {

		int pageType = -1;
		int pos = page_num * PAGE_SIZE;
		try {
			file.seek(pos);
			pageType = file.readByte();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return pageType;

	}

	public static int get_numRecs(RandomAccessFile file, int page_num) {
		int numRecs = 0;
		int pos = page_num * PAGE_SIZE + 2;
		try {
			file.seek(pos);
			numRecs = file.readShort();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return numRecs;
	}

	public static int get_conStart(RandomAccessFile file, int page_num) {

		int conStart = 512;
		int pos = page_num * PAGE_SIZE + 4;
		try {
			file.seek(pos);
			conStart = file.readShort();
			if (conStart == 0) {
				conStart = 512;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conStart;
	}

	public static int getRightMostChild(RandomAccessFile file, int page_num) {
		int rChild = 0;

		int pos = page_num * PAGE_SIZE + 6;
		try {
			file.seek(pos);
			rChild = file.readInt();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rChild;
	}

	public static int getParent(RandomAccessFile file, int page_num) {
		int parent = -1;
		int pos = page_num * PAGE_SIZE + 10;
		try {
			file.seek(pos);
			parent = file.readInt();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return parent;
	}

	public static int get_freePage(RandomAccessFile file) {
		int page = 0;
		int node = 0;
		while (getParent(file, node) != 0xFFFFFFFF) {

			node = getParent(file, node);
		}

		if (node != 0) {
			while (getRightMostChild(file, node) != 0xFFFFFFFF) {

				node = getRightMostChild(file, node);
			}

			page = node;
		}

		return page;
	}

	public static boolean check_overflow(RandomAccessFile file, int page_num, int rec_size) {

		int header_size = 16;
		int numRecs = get_numRecs(file, page_num);
		int con_start = get_conStart(file, page_num);
		int current_size = header_size + (numRecs * 2) + (PAGE_SIZE - con_start);

		if (rec_size + 2 < PAGE_SIZE - current_size)
			return false;
		return true;
	}

	public static int createParentPage(RandomAccessFile file, int page_num) {

		int numPages = get_numPages(file);
		int parent = numPages;

		try {
			file.setLength(file.length() + PAGE_SIZE);
			file.seek(file.length() - PAGE_SIZE);
			file.writeByte(INTERIOR);
			file.skipBytes(5);
			file.writeInt(page_num);// rightmost child --> new page created
			file.writeInt(0XFFFFFFFF); // Root page

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return parent;

	}

	public static int createLeafPage(RandomAccessFile file, int page_num) {

		int newPageNum = get_numPages(file);
		try {

			file.seek(PAGE_SIZE * page_num + 6); // write the sibling
			file.writeInt(newPageNum);
			file.setLength(file.length() + PAGE_SIZE);
			int parent = getParent(file, page_num);
			if (parent == 0xFFFFFFFF) {
				parent = createParentPage(file, newPageNum);

				file.seek(page_num * PAGE_SIZE + 10);
				file.writeInt(parent);

			}

			writeInteriorPage(file, page_num, newPageNum, parent);
			file.seek((newPageNum) * PAGE_SIZE);
			file.writeByte(LEAF);
			file.skipBytes(5);
			file.writeInt(0xFFFFFFFF);
			file.writeInt(parent);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return newPageNum;
	}

	public static int createInteriorSibling(RandomAccessFile file, int page_num) {
		int newPageNum = get_numPages(file);
		try {

			file.setLength(file.length() + PAGE_SIZE);
			int parent = getParent(file, page_num);
			if (parent == 0xFFFFFFFF) {
				parent = createParentPage(file, newPageNum);
				file.seek(page_num * PAGE_SIZE + 10);
				file.writeInt(parent);
				
			}
			writeInteriorPage(file, page_num,newPageNum, parent);
			file.seek(newPageNum * PAGE_SIZE);
			file.writeByte(INTERIOR);
			file.skipBytes(5);
			file.writeInt(0xFFFFFFFF);
			file.writeInt(parent);

		} catch (IOException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}
		return newPageNum;
	}

	public static void writeInteriorPage(RandomAccessFile file, int page_num, int right_most, int interior) {

		try {

			int numRecs = get_numRecs(file, interior);

			int rowid = get_rowID(file, page_num);

			if (check_overflow(file, interior, 10)) {

				interior = createInteriorSibling(file, interior);
				numRecs=0;

			}

			file.seek(interior * PAGE_SIZE);
			file.skipBytes(2);
			file.writeShort(numRecs + 1);

			int offset = file.readShort();

			if (offset == 0)
				offset = 512;

			int new_offset = offset - 8;

			file.writeInt(right_most);

			file.seek(interior * PAGE_SIZE + 4);
			file.writeShort(new_offset);

			file.skipBytes(10 + numRecs * 2);
			file.writeShort(new_offset);

			file.seek(interior * PAGE_SIZE + new_offset);
			file.writeInt(page_num);
			file.writeInt(rowid);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static int get_rowID(RandomAccessFile file, int page) {
		int rowid = 0;

		int conStart = get_conStart(file, page);
		int pageType = getPageType(file, page);
		try {

			file.seek((PAGE_SIZE * page) + conStart);
			if (pageType != LEAF) {
				file.skipBytes(2);
			}
			file.skipBytes(2);
			rowid = file.readInt();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return rowid;
	}

}
