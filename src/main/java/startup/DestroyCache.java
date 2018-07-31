package startup;

import model.BitmapHour;
import model.Data;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

public class DestroyCache {

    public static void main(String[] args) {


        Ignite ignite = Ignition.start("BitmapCluster-client.xml");

        IgniteCache<Long, BitmapHour> bitmaphour = ignite.getOrCreateCache("BitmapHourCache");
        IgniteCache<Long, BitmapHour> bitmapminute = ignite.getOrCreateCache("BitmapMinuteCache");
        IgniteCache<Long, Data> datacache = ignite.getOrCreateCache("DataCache");
        bitmaphour.destroy();
        bitmapminute.destroy();
        datacache.destroy();

    }
}
