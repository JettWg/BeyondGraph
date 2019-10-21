package org.beyond.graph.bulk.loading.storage;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class KVStorage {
    private static final Logger LOG = LoggerFactory.getLogger(KVStorage.class);

    static {
        RocksDB.loadLibrary();
    }

    private ColumnFamilyOptions cfOpts;
    private RocksDB rocksDB;
    private String dbPath;
    private Set<String> labels;
    private Map<String, ColumnFamilyHandle> cfHandleMap = new HashMap<>();
    private static KVStorage instance = null;

    public KVStorage(String rootDir, String graphName, Set<String> labels) throws RocksDBException {
        open(rootDir, graphName, labels);
        instance = this;
    }

    public static void initialize(String rootDir, String graphName, Set<String> labels) throws RocksDBException {
        new KVStorage(rootDir, graphName, labels);
    }

    public static KVStorage getInstance() {
        if (null == instance) {
            final String errMsg = "You may not instantiate a KVStorage";
            throw new RuntimeException(errMsg);
        }
        return instance;
    }

    public void set(String label, String key, String innerID) throws RocksDBException {
        ColumnFamilyHandle cfHandle = cfHandleMap.getOrDefault(label, null);
        if (null == cfHandle) {
            throw new RuntimeException("Label: " + label + " is not existed in nodeLabels: " +labels);
        }

        rocksDB.put(cfHandle, key.getBytes(), innerID.getBytes());
    }

    public void multiSets(String label, Map<String, String> idMappings) throws RocksDBException {
        ColumnFamilyHandle cfHandle = cfHandleMap.getOrDefault(label, null);
        if (null == cfHandle) {
            throw new RuntimeException("Label: " + label + " is not existed in nodeLabels: " +labels);
        }

        for (Map.Entry<String, String> kv : idMappings.entrySet()) {
            rocksDB.put(cfHandle, kv.getKey().getBytes(), kv.getValue().getBytes());
        }
    }

    public String get(String label, String key) throws RocksDBException {
        ColumnFamilyHandle cfHandle = cfHandleMap.getOrDefault(label, null);
        if (null == cfHandle) {
            throw new RuntimeException("Label: " + label + " is not existed in nodeLabels: " +labels);
        }

        byte[] b = rocksDB.get(cfHandle, key.getBytes());
        return null != b ? new String(b) : "";
    }

    public Map<String, String> multiGets(String label, List<String> keys) throws RocksDBException {
        ColumnFamilyHandle cfHandle = cfHandleMap.getOrDefault(label, null);
        if (null == cfHandle) {
            throw new RuntimeException("Label: " + label + " is not existed in nodeLabels: " +labels);
        }

        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        List<byte[]> keyByteList = new ArrayList<>();
        for (String k : keys) {
            keyByteList.add(k.getBytes());
            handleList.add(cfHandle);
        }

        List<byte[]> values = rocksDB.multiGetAsList(handleList, keyByteList);
        if (keyByteList.size() != values.size()) {
            throw new RuntimeException("RocksDB multiGets result.size: " + values.size() + " != " + "keys.size: " + keyByteList.size());
        }

        int length = keyByteList.size();
        Map<String, String> idMappings = new HashMap<>();
        for (int i = 0; i < length; i++) {
            if (values.get(i) == null) {
                // ignore null key, don't record it
                continue;
            }
            idMappings.put(keys.get(i), new String(values.get(i)));
        }

        return idMappings;
    }

    public void open(String rootDir, String graphName, Set<String> labels) throws RocksDBException {
        LOG.info("*** Open RocksDB ***");
        this.labels = labels;
        dbPath = Paths.get(rootDir, graphName).toString();

        cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        Options options = new Options();
        options.setCreateIfMissing(true);
        List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);
        if (cfs.size() > 0) {
            for (byte[] cf : cfs) {
                cfDescriptors.add(new ColumnFamilyDescriptor(cf ,cfOpts));
            }

            List<String> cfExistedNames = cfs.stream().map(String::new).collect(Collectors.toList());
            for (String label : labels) {
                if (!cfExistedNames.contains(label)) {
                    cfDescriptors.add(new ColumnFamilyDescriptor(label.getBytes(), cfOpts));
                }
            }
        } else {
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
            for (String label : labels) {
                cfDescriptors.add(new ColumnFamilyDescriptor(label.getBytes(), cfOpts));
            }
        }

        final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
        final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        rocksDB = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandleList);

        for (int i = 0; i < cfDescriptors.size(); i++) {
            cfHandleMap.put(new String(cfDescriptors.get(i).getName()), cfHandleList.get(i));
        }
    }

    public void close() {
        LOG.info("*** Close RocksDB ***");
        for (final ColumnFamilyHandle cfHandle : cfHandleMap.values()) {
            cfHandle.close();
        }
        rocksDB.close();
    }

    public void drop() throws RocksDBException {
        this.close();
        LOG.info("*** Drop RocksDB ***");
        RocksDB.destroyDB(dbPath, new Options());
    }
}
