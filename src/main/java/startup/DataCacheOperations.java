package startup;

import model.Data;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.List;
import static startup.BitmapOperations.UpdateBitmapHour;
import static startup.BitmapOperations.UpdateBitmapMinute;
import static startup.Expt.InsertIntoData;
import static startup.Expt.getRowsByTableName;
import static startup.QueryExecuter.ignite;
public class DataCacheOperations {


    //Update Data Cache for query on appliance data
    public static void UpdateDataCacheApp(List <List<Double>> strlist,List<List<String>> strliststr, List<Double> listTs, String[] columns,Long TS1, Long TS2, String sensor_id,String gran)
    {
        IgniteCache<Long, Data> datacache = ignite.getOrCreateCache("DataCache");
        List<Data> dl=new ArrayList<>();
        long tm = System.currentTimeMillis();
        int j=0,i=0;
        for(List<Double> str:strlist)
        {
            i=0;
            for(Double p:str)
            {

                if(j==0)
                {
                    Data dt= new Data();
                    dl.add(dt);
                    dl.get(i).setAvgW(p);
                    dl.get(i).setSensor_id(sensor_id);
                    dl.get(i).setGranularity(gran);

                }
                else if(j==1)
                {

                    dl.get(i).setTS(p);
                }
                i=i+1;
            }
            j=j+1;
        }
        j=0;
        for(List<String> str2:strliststr)
        {
            i=0;
            for(String p:str2)
            {
                if(j==0)
                {
                    dl.get(i).setAction(p);
                }
                else if(j==1)
                {

                    dl.get(i).setAppliances(p);
                }
                else if(j==2)
                {

                    dl.get(i).setUser(p);
                }
                i=i+1;
            }
            j=j+1;
        }

        for(j=0;j<dl.size();j++)
        {
           InsertIntoData(datacache,dl.get(j));
        }
        long tm1 = System.currentTimeMillis();
        System.out.println("UPDATE DATA CACHE TIME "+(tm1-tm));
//        ShowDataCache();
        if(gran.equals("minute"))
        UpdateBitmapMinute(columns,TS1, TS2, sensor_id);
        else
            UpdateBitmapHour(columns,TS1,TS2,sensor_id);
    }


    //Update Data Cache for all other queries.
    public static void UpdateDataCache(List <List<Double>> strlist,List<Double> listTs, String[] columns,Long TS1, Long TS2, String sensor_id,String gran)
    {
        IgniteCache<Long, Data> datacache = ignite.getOrCreateCache("DataCache");
        List<Data> dl=new ArrayList<>();
        long tm = System.currentTimeMillis();
        int j=0,i=0;
        for(List<Double> str:strlist)
        {
            i=0;
            for(Double p:str)
            {
                if(j==0)
                {
                    Data dt= new Data();
                    dl.add(dt);
                    dl.get(i).setAvgW(p);
                    dl.get(i).setSensor_id(sensor_id);
                    dl.get(i).setGranularity(gran);

                }
                else if(j==1)
                {
                    dl.get(i).setAvgV(p);
                }
                else if(j==2)
                {

                    dl.get(i).setTS(p-1800);
                }
                i=i+1;
            }
            j=j+1;
        }
        for(j=0;j<dl.size();j++)
        {
            InsertIntoData(datacache,dl.get(j));
        }

        long tm1 = System.currentTimeMillis();
        System.out.println("UPDATE DATA CACHE TIME "+(tm1-tm));
//        ShowDataCache();
        if(gran.equals("hour"))
        UpdateBitmapHour(columns,TS1, TS2, sensor_id);
        else
            UpdateBitmapMinute(columns,TS1,TS2,sensor_id);
    }

//Show all contents of the Data cache
    public static void ShowDataCache()
    {
        IgniteCache<Long, Data> cache = ignite.getOrCreateCache("DataCache");

        QueryCursor<List<?>> cursor=
                cache.query(
                        new SqlFieldsQuery("select * from Data"));
        QueryCursor<List<?>> cursorcount=
                cache.query(
                        new SqlFieldsQuery("select count(*) from Data"));

        System.out.println("PRINTED CURSOR "+ cursor.getAll()+"\n PRINTED COUNT "+cursorcount.getAll());
    }

    public static void UpdateDataTable(Dataset<Row> dt,String[] columns,Long TS1,Long  TS2,String sensor_id) {


        dt.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), "BitmapCluster-client.xml")
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "DATA1")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "TS")
                .mode(SaveMode.Overwrite)
                .save();


        System.out.println("d");

        ShowDataCache();

//        UpdateBitmapMinute(columns,TS1,TS2,sensor_id);

    }


    //dummy function for spark-ignite shared rdd.
    public static void funcignitespark() {
        SparkSession sparkSession = SparkSession.builder()
//                .master("spark://io:7077")
                .master("local[4]")
                .config("spark.logConf", true)
                .getOrCreate();


        Dataset<Row> df = getRowsByTableName(sparkSession, "sch_3");

        IgniteCache cache = ignite.getOrCreateCache("DataCache");

        df = df.where("TS>=1509820200 and TS<=1509820300 and sensor_id='power_k_seil_a'").select("W", "TS", "sensor_id", "V1", "FwdWh");

        df = df.withColumnRenamed("W", "avgW");


        df.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())

                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), "BitmapCluster-client.xml")
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "DATA1")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "TS")
                .mode(SaveMode.Append)
                .save();


        System.out.println("d");

        ShowDataCache();

    }
}
