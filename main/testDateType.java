import java.text.SimpleDateFormat;
import java.util.*;
import java.time.LocalTime;
import java.io.RandomAccessFile;
public class testDateType{
	public static void main(String[] args) throws Exception {
	String datestr = "2009-11-11";
	Date dt1 = new SimpleDateFormat("yyyy-MM-dd").parse(datestr);
	System.out.println(" inputed date String of format yyyy-MM-dd ");
	System.out.println(datestr + "\t" + dt1);
	System.out.println();
	long date = dt1.getTime();
	
	System.out.println(" Long type date ");
	System.out.println("" + date);
	System.out.println();
	String timestr = "20:15:08";
	
	LocalTime lt = LocalTime.parse(timestr);
	
	System.out.println(" Local time ");
	System.out.println("" + lt);
	System.out.println();
	int millis = 1000 * (lt.getHour() * 3600 + lt.getMinute() * 60 + lt.getSecond());
	System.out.println(" Millis ==> ");
	System.out.println("" + millis);
	System.out.println();
	
	Date dt2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2009-11-11 20:15:08");
	long datetime = dt2.getTime();
	System.out.println(" Date time ");
	System.out.println("" + datetime);
	System.out.println();
	
	RandomAccessFile raf = new RandomAccessFile("testDateTime.tbl", "rw");
	raf.writeLong(date);
	raf.writeInt(millis);
	raf.writeLong(datetime);
	raf.seek(0);
	long dp1 = raf.readLong();
	int dp2 = raf.readInt();
	long dp3 = raf.readLong();
	Date tmpdate = new Date(dp1);
	System.out.println(" read date : ");
	System.out.println(tmpdate);
	System.out.println();
	dp2 = dp2/1000;
	int hr = dp2/3600;
	int min = (dp2%3600)/60;
	int sec = dp2 % 60;
	System.out.println(" read Time ");
	System.out.println("" +hr+ ":" +min+ ":" +sec);
	System.out.println();
	
	System.out.println(" READ DATE TIME");
	Date tmpdatetime = new Date(dp3);
	System.out.println(tmpdatetime);
	System.out.println();
	}

}
