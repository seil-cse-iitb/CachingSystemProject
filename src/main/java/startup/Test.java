package startup;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.ignite.IgniteSparkSession;

public class Test {

    public static void main(String[] args) throws AnalysisException {

        // Using SparkBuilder provided by Ignite.
        IgniteSparkSession igniteSession = IgniteSparkSession.builder()
                .appName("Spark Ignite catalog example")
                .master("local")
                .config("spark.executor.instances", "2")
                //Only additional option to refer to Ignite cluster.
                .igniteConfig("BitmapCluster-client.xml")
                .getOrCreate();

// This will print out info about all SQL tables existed in Ignite.
//        igniteSession.catalog().listTables().show();
        // This will print out schema of PERSON table.
//        igniteSession.catalog().listColumns("DATA1").show();
        igniteSession.sql("select count(*) from DATA1").show();


    }
}
