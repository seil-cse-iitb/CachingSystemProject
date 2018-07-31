package startup;

import javafx.util.Pair;
import model.BitmapHour;
import model.BitmapMinute;
import model.Data;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.util.ArrayList;
import java.util.List;

import static startup.Expt.*;
import static startup.QueryExecuter.ignite;
import static config.Handler.HourBasev;
import static config.Handler.MinuteBasev;

public class BitmapOperations {

    //Returns time ranges as ArrayList which are not in the cache.
    public static ArrayList<Pair<Long,Long>> getDBTimeRanges(String bitString, Long TS1, Long TS2)
    {
        ArrayList<Pair<Long,Long>> dbTSs= new ArrayList<Pair<Long,Long>>(); // ArrayList of 0-range pairs
        Long i=0L,sv=0L,lv=0L;
        boolean b= false;
        for(i=0L;i<bitString.length();i++)
        {
            if(bitString.charAt(i.intValue())=='0')
            {
                if(!b)
                {
                    sv=i;
                    b=true;
                }
            }
            else
            {
                if(b)
                {
                    lv=i-1;
                    Pair <Long,Long> p= new Pair<Long,Long>(sv+TS1,lv+TS1);
                    dbTSs.add(p);

                    b=false;
                }
            }

        }
        if(bitString.length()-1>=0 && bitString.charAt(bitString.length()-1)=='0')
        {
            Pair<Long,Long> p =new Pair<Long,Long>(sv+TS1,TS2);
            dbTSs.add(p);
        }

        //Returns time ranges which are not in cache.
        return  dbTSs;

    }

    //Returns a the broken query to be executed on the database for hourly aggregated data values.
    public static String BreakQuery(ArrayList<Pair<Long,Long>> dbTSs)
    {
        String dbQuery="";
        for(int i1=0;i1<dbTSs.size();i1++){
            Long qwe=HourBasev+(dbTSs.get(i1).getValue()*3600);
            Long qwe1=(HourBasev+(dbTSs.get(i1).getKey()*3600 ));
            if(!qwe.equals(qwe1))
                dbQuery+="(TS >= "+qwe1+" and  TS < "+(qwe+3600)+" ) ";
            else
                dbQuery+="(TS >= "+qwe+" and TS<"+(qwe+3600)+" )";

            System.out.println(qwe1+"   "+qwe+"    "+dbTSs.get(i1).getKey()+"   "+dbTSs.get(i1).getValue());

            if(i1!=dbTSs.size()-1)
            {
                dbQuery+=" or ";
            }
        }
        return dbQuery;
    }

    //Returns a the broken query to be executed on the database for minutely aggregated data values.
    public static String BreakQueryMinute(ArrayList<Pair<Long,Long>> dbTSs)
    {
        String dbQuery="";
        for(int i1=0;i1<dbTSs.size();i1++){
            Long qwe=MinuteBasev+(dbTSs.get(i1).getValue()*60);
            Long qwe1=(MinuteBasev+(dbTSs.get(i1).getKey()*60 ));
            if(!qwe.equals(qwe1))
                dbQuery+="(TS >= "+qwe1+" and  TS < "+(qwe+60)+" ) ";
            else
                dbQuery+="(TS >= "+qwe+" and TS<"+(qwe+60)+" )";

            System.out.println(qwe1+"   "+qwe+"    "+dbTSs.get(i1).getKey()+"   "+dbTSs.get(i1).getValue());

            if(i1!=dbTSs.size()-1)
            {
                dbQuery+=" or ";
            }
        }
        return dbQuery;
    }


    //Function to perform AND operation of an Array of Strings, pass Array of BitString as input
    public static String ANDofStrings(String[] a,Long siz)
    {
        String bitString="";
        boolean b= false;
        Long sizeofString=siz;
        for(int j=0;j<sizeofString.intValue();j++) {
            for (int it = 0; it < 1; it++) {
                if (a[it].charAt(j) == '0') {
                    bitString += "0";
                    b=true;
                    break;
                }
            }
            if(!b)
            {
                bitString+="1";

            }
            else b=false;
        }
        return bitString;
    }

    //Check Minute Bitmap
    public static void CheckBitmapMinute(IgniteCache<Long, BitmapMinute> cache, Long TS1, Long TS2, String sensor_id, String[] columns)
    {

        String gran= "minute";
        long tm = System.currentTimeMillis();
        int numcols= columns.length;
        boolean allOne=true;
        Long oldTS1=TS1;
        Long oldTS2=TS2;
        TS1=(TS1-MinuteBasev)/60;
        TS2=(TS2-MinuteBasev)/60;

        //Create appended columns to form a query for bitmap
        String colsapp="";
        for(int it=0;it<numcols;it++)
        {
            colsapp+="substr("+columns[it]+","+(TS1)+","+(TS2-TS1+1)+") ";
            if(it==numcols-1)
                colsapp+=" ";
            else colsapp+=" , ";
        }
        //Create appended columns to form a query for database
        String colsapp1="";
        for(int it=0;it<numcols;it++)
        {
            colsapp1+=" " + columns[it]+"";
            if(it==numcols-1)
                colsapp1+=" ";
            else colsapp1+=", ";
        }

        tm = System.currentTimeMillis();
        QueryCursor<List<?>> cursor=
                cache.query(
                        new SqlFieldsQuery("select "+colsapp + "from BitmapMinute where sensor_id='"+sensor_id+"'"));
        long tm1 = System.currentTimeMillis();

        System.out.println("BITMAP FETCH TIME "+(tm1-tm));
        String qwe[]= new String[numcols];
        for(List<?> r:cursor)
        {
            for(int i=0;i<numcols;i++)
            {
                qwe[i] = (String)r.get(i);
                System.out.println(qwe[i].length());
            }
        }

        tm = System.currentTimeMillis();
        String bitString=ANDofStrings(qwe,(TS2-TS1+1));

        for(int as=0;as <bitString.length();as++)
        {
            if(bitString.charAt(as)=='0')
            {
                allOne=false;
            }
        }

        ArrayList<Pair<Long,Long>> dbTSs=new ArrayList<>();
        dbTSs=getDBTimeRanges(bitString,TS1,TS2);

        String sqlQ=BreakQueryMinute(dbTSs);

        tm1 = System.currentTimeMillis();
        System.out.println("PARSE QUERY TIME : "+ (tm1-tm));

        String cacheQ="select "+colsapp1+" from Data where TS>="+oldTS1+" and TS<="+oldTS2+" and sensor_id='"+sensor_id+"'"+" and granularity='"+gran+"'";
        String cacheQ1="select count(*) from Data where TS>="+oldTS1+" and TS<="+oldTS2+" and sensor_id='"+sensor_id+"'"+" and granularity='"+gran+"'";
        ExecuteonCache(cacheQ,cacheQ1);
        boolean flag=false;

        if(!allOne)
        {
            for(String c : columns) {
                if (c.equals("appliances")) {
                    ExecuteonDBApp(sqlQ, columns, TS1, TS2, sensor_id, gran);
                    flag = true;
                    break;
                }
                if (c.equals("temperature")) {
                    ExecuteonDBTempJoin(sqlQ, columns, TS1, TS2, sensor_id, gran);
                    flag = true;
                    break;
                }
            }
            if(!flag)
            {
                ExecuteonDB(sqlQ,columns,TS1,TS2,sensor_id,gran);
            }
        }
    }

    //Updates the Minute Bitmap
    public static void UpdateBitmapMinute(String[] columns,Long TS1, Long TS2,String sensor_id)
    {
        IgniteCache<Long, BitmapMinute> cache=ignite.getOrCreateCache("BitmapMinuteCache");
        long tm = System.currentTimeMillis();
        int numcols= columns.length;
        String colsapp="";
        for(int it=0;it<numcols;it++)
        {
            colsapp+=columns[it];
            if(it==numcols-1)
                colsapp+=" ";
            else colsapp+=" , ";

        }
        String colsapp1="";
        for(int it=0;it<numcols;it++)
        {
            colsapp1+=" " + columns[it]+",";
            if(it==numcols-1)
                colsapp1+=" ";
            else colsapp1+=" , ";

        }

        QueryCursor<List<?>> cursor=
                cache.query(
                        new SqlFieldsQuery("select "+ colsapp + "from BitmapMinute where sensor_id='"+sensor_id+"'"));
        String qwe="",appstr="";
        int colnum=0;
        for(String p : columns)
        {
            switch(p)
            {
                case "temperature": colnum=3;break;
                case "avgV": colnum=1; break;
                case "appliance": colnum=4; break;
                default: colnum=0;

            }

        }
        for(List<?> r:cursor)
        {
            qwe = (String)r.get(colnum);
        }

        for(Long i=TS1;i<=TS2;i++)
        {
            appstr+="1";
        }

        String newString=qwe.substring(0,TS1.intValue())+appstr+qwe.substring(TS2.intValue()+1,qwe.length());

        String quer="update BitmapMinute set ";
        for(int i=0;i<numcols;i++) {
            if(i<numcols-1)
                quer+=columns[i]+"='"+newString+"', ";
            else
                quer+=columns[i]+"='"+newString+"' ";

        }

        cache.query(
                new SqlFieldsQuery(quer+" where sensor_id='"+sensor_id+"'")
        );
        long tm1 = System.currentTimeMillis();
        System.out.println("UPDATE BITMAP TIME "+(tm1-tm));
    }

    //Update Hour Bitmap
    public static void UpdateBitmapHour(String[] columns,Long TS1, Long TS2,String sensor_id)
    {
        IgniteCache<Long, BitmapHour> cache=ignite.getOrCreateCache("BitmapHourCache");
        long tm = System.currentTimeMillis();
        int numcols= columns.length;
        String colsapp="";
        for(int it=0;it<numcols;it++)
        {
            colsapp+=columns[it];
            if(it==numcols-1)
                colsapp+=" ";
            else colsapp+=" , ";

        }
        String colsapp1="";
        for(int it=0;it<numcols;it++)
        {
            colsapp1+=" " + columns[it]+",";
            if(it==numcols-1)
                colsapp1+=" ";
            else colsapp1+=" , ";

        }

        System.out.println("colsapp" +colsapp);
        System.out.println("colsapp1" +colsapp);
        QueryCursor<List<?>> cursor=
                cache.query(
                        new SqlFieldsQuery("select "+ colsapp + "from BitmapHour where sensor_id='"+sensor_id+"'"));
        String qwe="",appstr="";
        for(List<?> r:cursor)
        {
            qwe = (String)r.get(0);
        }

        for(Long i=TS1;i<=TS2;i++)
        {
            appstr+="1";
        }


        String newString=qwe.substring(0,TS1.intValue())+appstr+qwe.substring(TS2.intValue()+1,qwe.length());

        String quer="update BitmapHour set ";
        for(int i=0;i<numcols;i++) {
            if(i<numcols-1)
                quer+=columns[i]+"='"+newString+"', ";
            else
                quer+=columns[i]+"='"+newString+"' ";

        }
        cache.query(
                new SqlFieldsQuery(quer+" where sensor_id='"+sensor_id+"'")
        );
        long tm1 = System.currentTimeMillis();

        System.out.println("UPDATE BITMAP TIME "+(tm1-tm));
    }

    //Check Hour Bitmap
    public static void CheckBitmap(IgniteCache<Long, BitmapHour> cache, Long TS1, Long TS2, String sensor_id, String[] columns) {
        String granularity = "hour";
        long tm = System.currentTimeMillis();
        int numcols = columns.length;
        boolean allOne = true;
        Long oldTS1 = TS1;
        Long oldTS2 = TS2;
        TS1 = (TS1 - HourBasev) / 3600;
        TS2 = (TS2 - HourBasev) / 3600;

        String colsapp = "";
        for (int it = 0; it < numcols; it++) {
            colsapp += "substr(" + columns[it] + "," + (TS1) + "," + (TS2 - TS1 + 1) + ") ";
            if (it == numcols - 1)
                colsapp += " ";
            else colsapp += " , ";

        }
        String colsapp1 = "";
        for (int it = 0; it < numcols; it++) {
            colsapp1 += " " + columns[it] + ",";
            if (it == numcols - 1)
                colsapp1 += " ";
            else colsapp1 += " , ";

        }

        System.out.println("colsapp" + colsapp);
        System.out.println("colsapp1" + colsapp1);

        QueryCursor<List<?>> cursor =
                cache.query(
                        new SqlFieldsQuery("select " + colsapp + "from BitmapHour where sensor_id='" + sensor_id + "'"));
        long tm1 = System.currentTimeMillis();

        System.out.println("BITMAP FETCH TIME " + (tm1 - tm));
        String qwe[] = new String[numcols];
        for (List<?> r : cursor) {

            for (int i = 0; i < numcols; i++) {
                qwe[i] = (String) r.get(i);
                System.out.println(qwe[i].length());
            }

        }
        tm = System.currentTimeMillis();
        String bitString = ANDofStrings(qwe, (TS2 - TS1 + 1));
        for (int as = 0; as < bitString.length(); as++) {
            if (bitString.charAt(as) == '0') {
                allOne = false;
            }
        }

        ArrayList<Pair<Long, Long>> dbTSs = new ArrayList<>();

        dbTSs = getDBTimeRanges(bitString, TS1, TS2);
        String sqlQ = BreakQuery(dbTSs);

        tm1 = System.currentTimeMillis();
        System.out.println("PARSE QUERY TIME : " + (tm1 - tm));

        String cacheQ = "select " + colsapp1 + " from Data where TS>=" + oldTS1 + " and TS<=" + oldTS2 + " and sensor_id='" + sensor_id + "'" + " and granularity='" + granularity + "'";
        String cacheQ1 = "select count(*) from Data where TS>=" + oldTS1 + " and TS<=" + oldTS2 + " and sensor_id='" + sensor_id + "'" + " and granularity='" + granularity + "'";
        ExecuteonCache(cacheQ, cacheQ1);
        boolean flag = false;
        if (!allOne) {
            for (String c : columns) {
                if (c.equals("appliances")) {
                    ExecuteonDBApp(sqlQ, columns, TS1, TS2, sensor_id, granularity);
                    flag = true;
                    break;
                }

                if (c.equals("temperature")) {
                    ExecuteonDBTempJoin(sqlQ, columns, TS1, TS2, sensor_id, granularity);
                    flag = true;
                    break;
                }

            }

            if (!flag) {
                ExecuteonDB(sqlQ, columns, TS1, TS2, sensor_id, granularity);
            }
        }
    }

    public static void ShowBitmapHourCache(int TS1, int TS2)
    {
        IgniteCache<Long, Data> cache = ignite.getOrCreateCache("BitmapHourCache");
//        substr(avgW,"+(TS1-2)+","+(TS2-TS1+7)+")
        QueryCursor<List<?>> cursor=
                cache.query(
                        new SqlFieldsQuery("select substr(avgW,"+(TS1+1)+","+(TS2-TS1)+") from BitmapHour"));

        System.out.println("PRINTED CURSOR "+ cursor.getAll());
    }
}
