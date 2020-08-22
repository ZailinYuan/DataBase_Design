import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.ArrayDeque;
import static java.lang.System.out;
import java.time.LocalTime;
import java.text.SimpleDateFormat;
public class InsertQuery{

	public static void InsertOperation(String table_name, ArrayList<String> insertrecord, ArrayList<String> dataTypes){
		String filename = "data/" + table_name + ".tbl";
		short PAGE_SIZE = 512;
		try{
			RandomAccessFile tablefile = new RandomAccessFile(filename, "rw");
			int page_offset = 0;
			int right_most_page_num = Page.get_freePage(tablefile);
			page_offset += PAGE_SIZE * right_most_page_num; //the right most child pointer is an integer of page number?
			// Now the pointer is at the right most leaf, calculate the required space first
			
			byte rec_length = 7;
			rec_length += dataTypes.size(); //record the length of a tuple, the cell header takes 6 bytes
			byte[] Data_types = new byte[dataTypes.size()]; //The record header
		
			for (int index = 0; index < dataTypes.size();index++){
				if(insertrecord.get(index).equals("NA")){
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
						Data_types[index] += rec_length;
						break;
					default:
						System.out.println("Error on insertion: unknown data type");
						return;
				}
			}
			//length is calculated, now check if the current page has enough space
			tablefile.seek(page_offset + 0x02);
			short rec_num = tablefile.readShort();
			short last_record = tablefile.readShort();
			int last_rowid = 0;
			if(last_record == 0) last_record = 512;//A newly created table without any record
			else{last_rowid = Page.get_rowID(tablefile, right_most_page_num);}
			if(last_record - rec_num*2 - 0x10 < rec_length){
				right_most_page_num = Page.createLeafPage(tablefile,right_most_page_num); //handle overflow, reset page offset pointer
				page_offset = PAGE_SIZE * right_most_page_num;
				rec_num = 0;
				last_record = PAGE_SIZE;
			}
			//update the page header
			tablefile.seek(page_offset + 0x02);
			rec_num += 1;
			tablefile.writeShort(rec_num);//update record counts
			last_record -= rec_length; //update last record pointer
			tablefile.writeShort(last_record); 
			tablefile.seek(page_offset + 0x10 + (rec_num-1) *2);
			tablefile.writeShort(last_record);//append the last record pointer in the record pointer array

			//next, write the record header
			tablefile.seek(page_offset + last_record);
			tablefile.writeShort(rec_length - 2);//payload
			tablefile.writeInt(last_rowid + 1); //rowid
			tablefile.writeByte(dataTypes.size());//number of columns
			for (byte b : Data_types){ // list of column data types
				tablefile.writeByte(b);
			}
                        for (int index = 0; index < dataTypes.size();index++){
				if(insertrecord.get(index).equals("NA")) continue;
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
						try{
							LocalTime lt = LocalTime.parse(insertrecord.get(index));
							int millis = 1000 * (lt.getHour() * 3600 + lt.getMinute() * 60 + lt.getSecond());
							tablefile.writeInt(millis);
						}
						catch(Exception e){
							System.out.println("time format error, must be HH:MM:SS");
						}
                                                break;
                                        case "DATETIME":
						try{
							Date dt = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(insertrecord.get(index));
							long datetime = dt.getTime();
							tablefile.writeLong(datetime);
						}
						catch(Exception e) {
							System.out.println("datetime format error, must be yyyy-MM-dd_HH:mm:ss");
						}
                                                break;
                                        case "DATE":
						try{
                                                        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(insertrecord.get(index));
                                                        long date_as_time = date.getTime();
                                                        tablefile.writeLong(date_as_time);
                                                }       
                                                catch(Exception e) {
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
		}
		catch(Exception e){
			System.out.println(e);
		}
	}
}
