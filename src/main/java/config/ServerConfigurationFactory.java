package config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

/** This file was generated by Ignite Web Console (07/18/2018, 15:31) **/
public class ServerConfigurationFactory {
    /**
     * Configure grid.
     * 
     * @return Ignite configuration.
     * @throws Exception If failed to construct Ignite configuration instance.
     **/
    public static IgniteConfiguration createConfiguration() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("BitmapCluster");

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();

        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47510"));

        discovery.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discovery);

        cfg.setCacheConfiguration(
            cacheBitmapHourCache(),
            cacheDataCache(),
            cacheBitmapMinuteCache()
        );

        return cfg;
    }

    /**
     * Create configuration for cache "BitmapHourCache".
     * 
     * @return Configured cache.
     **/
    public static CacheConfiguration cacheBitmapHourCache() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("BitmapHourCache");
        ccfg.setGroupName("BitmapGroup");
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(1);
        ccfg.setReadFromBackup(true);
        ccfg.setCopyOnRead(true);
        ccfg.setSqlSchema("Bitmap");

        ArrayList<QueryEntity> qryEntities = new ArrayList<>();

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType("java.lang.Long");
        qryEntity.setValueType("model.BitmapHour");
        qryEntity.setTableName("bitmaphour");
        qryEntity.setKeyFieldName("id");
        qryEntity.setValueFieldName("Long");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("avgW", "java.lang.String");
        fields.put("avgV", "java.lang.String");
        fields.put("sensor_id", "java.lang.String");
        fields.put("appliances", "java.lang.String");
        fields.put("temperature", "java.lang.String");
        fields.put("id", "java.lang.Long");
        fields.put("Long", "model.BitmapHour");

        qryEntity.setFields(fields);

        ArrayList<QueryIndex> indexes = new ArrayList<>();

        QueryIndex index = new QueryIndex();

        index.setName("id");
        index.setIndexType(QueryIndexType.SORTED);

        LinkedHashMap<String, Boolean> indFlds = new LinkedHashMap<>();

        indFlds.put("sensor_id", true);

        index.setFields(indFlds);
        indexes.add(index);

        qryEntity.setIndexes(indexes);
        qryEntities.add(qryEntity);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /**
     * Create configuration for cache "DataCache".
     * 
     * @return Configured cache.
     **/
    public static CacheConfiguration cacheDataCache() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("DataCache");
        ccfg.setGroupName("DataGroup");
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCopyOnRead(true);

        ArrayList<QueryEntity> qryEntities = new ArrayList<>();

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType("java.lang.Long");
        qryEntity.setValueType("model.Data");
        qryEntity.setTableName("data");
        qryEntity.setKeyFieldName("id");
        qryEntity.setValueFieldName("Long");

        HashSet<String> keyFields = new HashSet<>();

        keyFields.add("TS");

        qryEntity.setKeyFields(keyFields);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("avgW", "java.lang.Double");
        fields.put("avgV", "java.lang.Double");
        fields.put("sensor_id", "java.lang.String");
        fields.put("granularity", "java.lang.String");
        fields.put("TS", "java.lang.Double");
        fields.put("temperature", "java.lang.Double");
        fields.put("appliances", "java.lang.String");
        fields.put("action", "java.lang.String");
        fields.put("user", "java.lang.String");
        fields.put("id", "java.lang.Long");
        fields.put("Long", "model.Data");

        qryEntity.setFields(fields);

        ArrayList<QueryIndex> indexes = new ArrayList<>();

        QueryIndex index = new QueryIndex();

        index.setName("id");
        index.setIndexType(QueryIndexType.SORTED);

        LinkedHashMap<String, Boolean> indFlds = new LinkedHashMap<>();

        indFlds.put("sensor_id", true);
        indFlds.put("granularity", true);
        indFlds.put("TS", true);

        index.setFields(indFlds);
        indexes.add(index);

        qryEntity.setIndexes(indexes);
        qryEntities.add(qryEntity);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }

    /**
     * Create configuration for cache "BitmapMinuteCache".
     * 
     * @return Configured cache.
     **/
    public static CacheConfiguration cacheBitmapMinuteCache() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("BitmapMinuteCache");
        ccfg.setGroupName("BitmapGroup");
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(1);
        ccfg.setReadFromBackup(true);
        ccfg.setCopyOnRead(true);

        ArrayList<QueryEntity> qryEntities = new ArrayList<>();

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType("java.lang.Long");
        qryEntity.setValueType("model.BitmapMinute");
        qryEntity.setTableName("bitmapminute");
        qryEntity.setKeyFieldName("id");
        qryEntity.setValueFieldName("Long");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("avgW", "java.lang.String");
        fields.put("avgV", "java.lang.String");
        fields.put("sensor_id", "java.lang.String");
        fields.put("temperature", "java.lang.String");
        fields.put("appliances", "java.lang.String");
        fields.put("user", "java.lang.String");
        fields.put("action", "java.lang.String");
        fields.put("id", "java.lang.Long");
        fields.put("Long", "model.BitmapMinute");

        qryEntity.setFields(fields);

        ArrayList<QueryIndex> indexes = new ArrayList<>();

        QueryIndex index = new QueryIndex();

        index.setName("id");
        index.setIndexType(QueryIndexType.SORTED);

        LinkedHashMap<String, Boolean> indFlds = new LinkedHashMap<>();

        indFlds.put("sensor_id", true);

        index.setFields(indFlds);
        indexes.add(index);

        qryEntity.setIndexes(indexes);
        qryEntities.add(qryEntity);

        ccfg.setQueryEntities(qryEntities);

        return ccfg;
    }
}