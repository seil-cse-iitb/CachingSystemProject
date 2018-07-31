package startup;

import javafx.util.Pair;
import model.BitmapHour;
import model.BitmapMinute;
import model.Data;
import model.Query;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import static startup.BitmapOperations.*;
import static startup.Expt.*;

public class QueryExecuter {


    public static Ignite ignite= Ignition.start("BitmapCluster-client.xml");
    public static void QueryHour(String sensor_id, Long TS1, Long TS2, String[] cols)
    {
        IgniteCache<Long, BitmapHour> cache = ignite.getOrCreateCache("BitmapHourCache");
        CheckBitmap(cache,TS1,TS2,sensor_id,cols);
    }

    public static void QueryMinute(String sensor_id, Long TS1, Long TS2, String[] cols)
    {
        IgniteCache<Long, BitmapMinute> cache = ignite.getOrCreateCache("BitmapMinuteCache");
        CheckBitmapMinute(cache,TS1,TS2,sensor_id,cols);
    }

    public static void main(String[] args) {

        logsOff();
        String[] cols={"avgW","appliances","user","action"};

        Query q1= new Query();

        q1.setCols(cols);
        q1.setSensor_id("power_k_seil_a");
        q1.setGranularity("minute");
        q1.setTS1(1507573800L);//10 oct
        q1.setTS2(1507746600L);//12 oct

        long tm = System.currentTimeMillis();
        System.out.println("NEXT QUERY");
        if(q1.getGranularity()=="minute")
        {
            QueryMinute(q1.getSensor_id(),q1.getTS1(),q1.getTS2(),q1.getCols());//10 oct 2018 to 12 oct 2018

        }
        else
        {
            QueryHour(q1.getSensor_id(),q1.getTS1(),q1.getTS2(),q1.getCols());
        }


        Query q2= new Query();

        q2.setCols(cols);
        q2.setSensor_id("power_k_seil_a");
        q2.setGranularity("minute");
        q2.setTS1(1507660200L);//10 oct
        q2.setTS2(1507746000L);//12 oct
        System.out.println("NEXT QUERY 2");
        if(q2.getGranularity()=="minute")
        {
            QueryMinute(q2.getSensor_id(),q2.getTS1(),q2.getTS2(),q2.getCols());//11 oct 2018 to 12 oct 2018

        }
        else
        {
            QueryHour(q2.getSensor_id(),q2.getTS1(),q2.getTS2(),q2.getCols());
        }
//        QueryMinute("power_k_seil_a",1507573800L,1510252200L,cols);//10 oct 2018 to 10 nov 2018
//        QueryMinute("power_k_seil_a",1507573800L,1507594800L,cols);//10 oct 2018 to 12 oct 2018
//        QueryMinute("power_k_seil_a",1507573800L,1507746600L,cols);//10 oct 2018 to 12 oct 2018




//


//
//        QueryMinute("power_k_seil_a",1516348457L,1516350000L,cols);//20 june 16: 22 june 14//1517479136L



//        try{InitCache.main(cols);}
//        catch (Exception e){}

        System.out.println("1NEXT QUERY");
//        QueryMinute("power_k_seil_a",1512239400L,1512844200L,cols); //dec 3, dec 10
        System.out.println("2NEXT QUERY");
//        QueryMinute("power_k_seil_a",1512498600L,1512930600L,cols); //dec 6, dec 11





        System.out.println("3NEXT QUERY");
//        QueryMinute("power_k_seil_a",1511807400L,1512498600L,cols);//28 nov 2017, 6 dec 2017




        System.out.println("4NEXT QUERY");
//        QueryMinute("power_k_seil_a",1511771400L,1512030600L,cols); //20 nov, 14: 30 nov 14

//        QueryMinute("power_k_seil_a",1511116200L,1513708200L,cols);//20 nov 2017, 20 dec 2017
        System.out.println("5NEXT QUERY");
//        QueryMinute("power_k_seil_a",1513276200L,1513621800L,cols); //dec 15, dec 19

        System.out.println("6NEXT QUERY");
//        QueryMinute("power_k_seil_a",1511116200L,1513708200L,cols);//20 nov 2017, 20 dec 2017


    }
}
