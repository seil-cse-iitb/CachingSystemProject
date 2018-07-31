package startup;

import config.handler.ConfigHandler;
import model.BitmapHour;
import model.BitmapMinute;
import model.Data;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static startup.DataCacheOperations.UpdateDataCache;
import static startup.DataCacheOperations.UpdateDataCacheApp;
import static startup.QueryExecuter.ignite;

public class Expt {




    public static void logsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", ConfigHandler.MYSQL_USERNAME);
        properties.setProperty("password", ConfigHandler.MYSQL_PASSWORD);
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        return properties;
    }
    public static Properties getProperties9() {
        Properties properties = new Properties();
        properties.setProperty("user", ConfigHandler.MYSQL_USERNAME9);
        properties.setProperty("password", ConfigHandler.MYSQL_PASSWORD9);
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        return properties;
    }

    public static Dataset<Row> getRowsByTableName(SparkSession sparkSession, String tableName) {
        Properties properties = getProperties();

        Dataset<Row> rows = sparkSession.read().jdbc(ConfigHandler.MYSQL_URL, tableName, properties);
        return rows;
    }
    public static Dataset<Row> getRowsByTableName9(SparkSession sparkSession, String tableName) {
        Properties properties = getProperties9();

        Dataset<Row> rows = sparkSession.read().jdbc(ConfigHandler.MYSQL_URL9, tableName, properties);
        return rows;
    }
        public static void InsertIntoData(IgniteCache<Long, Data> cachename, Data bm) {

        QueryCursor<List<?>> cursor =cachename.query(new SqlFieldsQuery("select MAX(id)+1 from Data"));
        Long qwe=0L;
        for(List<?> r:cursor)
        {
            qwe = (Long) r.get(0);

        }
        if(qwe==null)
        {
            qwe=0L;
        }
        boolean flag = true;
        QueryCursor<List<?>> cursorTS =cachename.query(new SqlFieldsQuery("select TS from Data"));

        if(flag) {
            cachename.query(
                    new SqlFieldsQuery("insert into Data (avgW,avgV,sensor_id,granularity,id,TS,appliances,action,user,temperature) values(" + bm.getAvgW() + " , " + bm.getAvgV() + " ,'" + bm.getSensor_id() + "','" + bm.getGranularity() + "'," + qwe + "," + bm.getTS() + ",'"+bm.getAppliances()+"','"+bm.getAction()+"','"+bm.getUser()+"',"+bm.getTemperature()+")"));
        }
    }


    public static void InsertIntoBitmapHour(IgniteCache<Long, BitmapHour> cachename, BitmapHour bm) {
        QueryCursor<List<?>> cursor =cachename.query(new SqlFieldsQuery("select MAX(id)+1 from BitmapHour"));
        Long qwe=0L;
        for(List<?> r:cursor)
        {
            qwe = (Long) r.get(0);
        }

        System.out.println("BITMPAINSERT "+bm.getAvgW());
        if(qwe==null)
        {
            qwe=0L;
        }
        cachename.query(
                new SqlFieldsQuery("insert into BitmapHour (avgW,avgV,sensor_id,id) values('"+bm.getAvgW()+"' , '"+bm.getAvgV()+"' ,'"+bm.getSensor_id()+"',"+qwe+")"));
    }

    public static void InsertIntoBitmapMinute(IgniteCache<Long, BitmapMinute> cachename, BitmapMinute bm) {

        QueryCursor<List<?>> cursor =cachename.query(new SqlFieldsQuery("select MAX(id)+1 from BitmapMinute"));
        Long qwe=0L;
        for(List<?> r:cursor)
        {
            qwe = (Long) r.get(0);
        }

        if(qwe==null)
        {
            qwe=0L;
        }
        cachename.query(

                new SqlFieldsQuery("insert into BitmapMinute (avgW,avgV,sensor_id,id,temperature,appliances,user,action) values('"+bm.getAvgW()+"' , '"+bm.getAvgV()+"' ,'"+bm.getSensor_id()+"',"+qwe+",'"+bm.getTemperature()+"','"+bm.getAppliances()+"','"+bm.getUser()+"','"+bm.getAction()+"')"));
    }

    public static void ExecuteonDB(String sqlQ,String[] columns,Long TS1,Long TS2, String sensor_id,String gran)
    {
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("spark://10.129.149.14:7077")
                .getOrCreate();
        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");
        System.out.println("sqlQ "+sqlQ);
        sch_3=sch_3.where(sqlQ);
        Column timestamp = functions.col("TS").cast(TimestampType).as("eventTime");
        String[] colsDB=new String[columns.length];
        int i=0;
        for(String p: columns) {
            switch (p) {
                case "avgW":
                    colsDB[i] = "W";
                    break;
                case "avgV":
                    colsDB[i] = "V1";
                    break;
            }
            i = i + 1;
        }
        sch_3=sch_3.groupBy(functions.window(timestamp,"1 "+gran)).avg(colsDB).sort("window");

        long tm = System.currentTimeMillis();


        sch_3.collect();
        long tm1 = System.currentTimeMillis();
        System.out.println("DB FETCH TIME "+ (tm1-tm));
        System.out.println("count from DB "+sch_3.count());

        tm= System.currentTimeMillis();
        List<Double> listOne = sch_3.select("avg(W)").as(Encoders.DOUBLE()).collectAsList();
        List<Double> listTwo = sch_3.select("avg(V1)").as(Encoders.DOUBLE()).collectAsList();
        List<Double> listTs = sch_3.select("window.start").as(Encoders.DOUBLE()).collectAsList();
        List<List <Double> > strlist= new ArrayList<>();
        int numcols=colsDB.length;
        for(i=0;i<=numcols+1;i++)
        {
            List <Double> str1= new ArrayList<>();
            if(i==0)
            {
                for(Double r : listOne)
                {
                    str1.add(r);
                }

            }
            if(i==1)
            {
                for(Double r : listTwo)
                {
                    str1.add(r);
                }

            }
            if(i==2)
            {
                for(Double r : listTs)
                {
                    str1.add(r);
                }

            }
            strlist.add(str1);
        }

        tm1= System.currentTimeMillis();
        System.out.println("CONVERT TO DATASET TIME "+(tm1-tm));
        UpdateDataCache(strlist,listTs,columns, TS1,TS2, sensor_id,gran);
    }

    public static void ExecuteonDBApp(String sqlQ,String[] columns,Long TS1,Long TS2, String sensor_id,String gran)
    {
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("spark://10.129.149.14:7077")
                .getOrCreate();

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");

        Dataset<Row> tsdt =sparkSession.emptyDataFrame();

        Dataset<Row> user_appliances = getRowsByTableName9(sparkSession, "portal_actions"),usernamedt,appliancesdt;
        System.out.println("sqlQ "+sqlQ+" and sensor_id='"+sensor_id+"'");
        sch_3=sch_3.where("("+sqlQ+") and sensor_id='"+sensor_id+"' and W<>0");

        sqlQ=sqlQ.replace("TS","timestamp");
        System.out.println("SQLQ  "+ sqlQ);
        user_appliances=user_appliances.where(sqlQ);

        Column timestamp = functions.col("TS").cast(TimestampType).as("eventTime");
        Column timestampu = functions.col("timestamp").cast(TimestampType).as("eventTime");
        String[] colsDB=new String[columns.length];
        int i=0;
        for(String p: columns)
        {
            switch(p){
                case "avgW": colsDB[i]="W"; break;
                case "avgV": colsDB[i]="V1";  break;
                case "temperature" : colsDB[i]="temperature"; break;
                case "appliances" : colsDB[i]="appliance"; break;
                case "action" : colsDB[i]="action"; break;
                case "user" : colsDB[i]="username"; break;
            }
            i=i+1;
        }

        sch_3=sch_3.groupBy(functions.window(timestamp,"1 "+gran)).avg("W").sort("window");
        Dataset<Row> userapp=user_appliances;

        userapp=userapp.groupBy(functions.window(timestampu,"1 "+gran))
                .agg(functions.concat_ws(",",functions.collect_list("appliance"),functions.collect_list("username"),functions.collect_list("action"))).sort("window");
        appliancesdt=user_appliances.groupBy(functions.window(timestampu,"1 "+gran))
                .agg(functions.concat_ws(",",functions.collect_list("appliance")).as("appliance")).sort("window").select("appliance","window");

        appliancesdt=appliancesdt.withColumnRenamed("window","window2");

        usernamedt=user_appliances.groupBy(functions.window(timestampu,"1 "+gran))
                .agg(functions.concat_ws(",",functions.collect_list("username")).as("username")).sort("window").select("username","window");


        usernamedt=usernamedt.withColumnRenamed("window","window3");

        user_appliances=user_appliances.groupBy(functions.window(timestampu,"1 "+gran))
                .agg(functions.concat_ws(",",functions.collect_list("action")).as("action")).sort("window");

        user_appliances=user_appliances.withColumnRenamed("window","window1");
        userapp=userapp.withColumnRenamed("window","window1");

        user_appliances=user_appliances.join(appliancesdt,user_appliances.col("window1").equalTo(appliancesdt.col("window2")));
        user_appliances=user_appliances.join(usernamedt,usernamedt.col("window3").equalTo(user_appliances.col("window1")));
//        sch_3.show();
//        temp_5.show();

        Dataset<Row>joineddt=sch_3.join(user_appliances,sch_3.col("window").equalTo(user_appliances.col("window1"))).select("window","avg(w)","action","appliance","username");

        long tm = System.currentTimeMillis();
        joineddt.show();
//        joineddt.show(1,false);
        long tm1 = System.currentTimeMillis();
        System.out.println("DB FETCH TIME "+ (tm1-tm));


//FOR INSERTION WITH SPARK_IGNITE SHARED RDD
/*
        Column windowstart_s = joineddt.col("window.start").cast("long");
        joineddt=joineddt.withColumnRenamed("avg(W)", "avgW");

        joineddt=joineddt.withColumn("TS",windowstart_s);
        joineddt=joineddt.drop("window");


        joineddt=joineddt.withColumn("temperature", functions.lit(0));
        joineddt=joineddt.withColumn("temperature",joineddt.col("temperature").cast(DataTypes.DoubleType));
        joineddt=joineddt.withColumn("granularity", functions.lit("minute"));
        joineddt=joineddt.withColumn("granularity",joineddt.col("granularity").cast(DataTypes.StringType));
        joineddt=joineddt.withColumn("sensor_id", functions.lit(sensor_id));
        joineddt=joineddt.withColumn("sensor_id",joineddt.col("sensor_id").cast(DataTypes.StringType));
        joineddt=joineddt.withColumn("avgV", functions.lit(0));
        joineddt=joineddt.withColumn("avgV",joineddt.col("avgV").cast(DataTypes.DoubleType));

        String [] arrr=joineddt.columns();
        for(String p:arrr)
        {
            System.out.println(p+" ");
        }

        joineddt.show(1,false);
        UpdateDataTable(joineddt,columns,TS1,TS2,sensor_id);*/

//***************************************************************************************************************************
        tm = System.currentTimeMillis();

       List<Double> listOne = joineddt.select("avg(W)").as(Encoders.DOUBLE()).collectAsList();
        List<String> listTwo = joineddt.select("action").as(Encoders.STRING()).collectAsList();
        List<String> listTwo1 = joineddt.select("appliance").as(Encoders.STRING()).collectAsList();
        List<String> listTwo2 = joineddt.select("username").as(Encoders.STRING()).collectAsList();
        List<Double> listTs = joineddt.select("window.start").as(Encoders.DOUBLE()).collectAsList();


        List<List <Double> > strlist= new ArrayList<>();
        List<List <String> > strliststr= new ArrayList<>();
        int numcols=colsDB.length;
        for(i=0;i<2;i++) {
            List<Double> str1 = new ArrayList<>();
            if (i == 0) {
                for (Double r : listOne) {
                    str1.add(r);
                }
            }
            if (i == 1) {
                for (Double r : listTs) {
                    str1.add(r);
                }
            }
            strlist.add(str1);
        }
        for(i=0;i<=2;i++) {
            List<String> str1 = new ArrayList<>();
            if (i == 0) {
                for (String r : listTwo) {
                    str1.add(r);
                }
            }
            if (i == 1) {
                for (String r : listTwo1) {
                    str1.add(r);
                }
            }
            if (i == 2) {
                for (String r : listTwo2) {
                    str1.add(r);
                }
            }
            strliststr.add(str1);
        }
        tm1 = System.currentTimeMillis();
        System.out.println("COVERT TO DATASET TIME "+(tm1-tm));

        UpdateDataCacheApp(strlist,strliststr,listTs,columns, TS1,TS2, sensor_id,gran);
    }


    public static void ExecuteonDBTempJoin(String sqlQ,String[] columns,Long TS1,Long TS2, String sensor_id,String gran)
    {
        System.out.println(sqlQ);
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("spark://10.129.149.14:7077")
                .getOrCreate();

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");
        Dataset<Row> temp_5 = getRowsByTableName(sparkSession, "temp_5");
        System.out.println("sqlQ "+sqlQ+" and sensor_id='"+sensor_id+"'");
        sch_3=sch_3.where("("+sqlQ+") and sensor_id='"+sensor_id+"' and W<>0");
        temp_5=temp_5.where("("+sqlQ+") and sensor_id='temp_k_213'");
        Column timestamp = functions.col("TS").cast(TimestampType).as("eventTime");
        String[] colsDB=new String[columns.length];
        int i=0;
        for(String p: columns)
        {
            switch(p){
                case "avgW": colsDB[i]="W"; break;
                case "avgV": colsDB[i]="V1";  break;
                case "temperature" : colsDB[i]="temperature"; break;
            }
            i=i+1;
        }

        sch_3=sch_3.groupBy(functions.window(timestamp,"1 "+gran)).avg("W").sort("window");
        temp_5=temp_5.groupBy(functions.window(timestamp,"1 "+gran)).avg("temperature").sort("window");
        temp_5=temp_5.withColumnRenamed("window","window1");

//        sch_3.show();
//        temp_5.show();

        Dataset<Row>joineddt=sch_3.join(temp_5,sch_3.col("window").equalTo(temp_5.col("window1")));
//        joineddt.show();
        long tm = System.currentTimeMillis();
        joineddt.collect();
        long tm1 = System.currentTimeMillis();
        System.out.println("DB FETCH TIME "+ (tm1-tm));

        joineddt.logicalPlan();
//        joineddt.write().format(IgniteDataFrameSettings.FORMAT_IGNITE()).save();
//        joineddt.write()
//                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
//                .mode(SaveMode.Ignore).save();


        System.out.println("count from DB "+joineddt.count());

        tm = System.currentTimeMillis();

        List<Double> listOne = joineddt.select("avg(W)").as(Encoders.DOUBLE()).collectAsList();
        List<Double> listTwo = joineddt.select("avg(temperature)").as(Encoders.DOUBLE()).collectAsList();
        List<Double> listTs = joineddt.select("window.start").as(Encoders.DOUBLE()).collectAsList();


        List<List <Double> > strlist= new ArrayList<>();
        int numcols=colsDB.length;
        for(i=0;i<=numcols+1;i++)
        {
            List <Double> str1= new ArrayList<>();
            if(i==0)
            {
                for(Double r : listOne)
                {
                    str1.add(r);
                }
            }
            if(i==1)
            {
                for(Double r : listTwo)
                {
                    str1.add(r);
                }
            }
            if(i==2)
            {
                for(Double r : listTs)
                {
                    str1.add(r);
                }
            }
            strlist.add(str1);
        }
        tm1 = System.currentTimeMillis();
        System.out.println("COVERT TO DATASET TIME "+(tm1-tm));

        UpdateDataCache(strlist,listTs,columns, TS1,TS2, sensor_id,gran);
    }

    public static void ExecuteonCache(String cacheQ, String cacheQ1)
    {
        IgniteCache<Long, Data> cache=ignite.getOrCreateCache("DataCache");
        long tm = System.currentTimeMillis();
        QueryCursor<List<?>> cursor=
                cache.query(
                        new SqlFieldsQuery(cacheQ));

        long tm1 = System.currentTimeMillis();
        System.out.println("DATA CACHE FETCH TIME "+(tm1-tm));
        QueryCursor<List<?>> cursorcount=
                cache.query(
                        new SqlFieldsQuery(cacheQ1));
        System.out.println("PRINTED CURSOR "+ cursor.getAll()+"\n PRINTED COUNT cache"+cursorcount.getAll());
    }


    public static void main(String[] args) {
        //Test Run
        //ExecuteonDB("sensor_id='power_k_seil_a' and TS>=1529483400 and TS<=1529505000");
    }
}
