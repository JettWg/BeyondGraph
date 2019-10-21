package org.beyond.graph.bulk.loading;

import org.junit.Test;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RocksDBTests {
    private static final String dbPath = "./kv_db/test_db";

    static {
        RocksDB.loadLibrary();
    }

    RocksDB rocksDB;

    @Test
    public void testDropDB() throws RocksDBException, IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        if (!Files.isSymbolicLink(Paths.get(dbPath)))
            Files.createDirectories(Paths.get(dbPath));

        rocksDB = RocksDB.open(options, dbPath);
        rocksDB.close();

        RocksDB.destroyDB(dbPath, options);
    }

    @Test
    public void testDefaultColumnFamily() throws RocksDBException, IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);

        if (!Files.isSymbolicLink(Paths.get(dbPath)))
            Files.createDirectories(Paths.get(dbPath));
        rocksDB = RocksDB.open(options, dbPath);

        /**
         * 简单 key-value
         */
        byte[] key = "Hello".getBytes();
        byte[] value = "World".getBytes();
        rocksDB.put(key, value);

        byte[] getValue = rocksDB.get(key);
        System.out.println(new String(value));

        /**
         * 通过List做主键查询
         */
        rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key);
        keys.add("SecondKey".getBytes());

        Map<byte[], byte[]> valueMap = rocksDB.multiGet(keys);
        for (Map.Entry<byte[], byte[]> entry : valueMap.entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }

        /**
         * 打印全部[key - value]
         */
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

        /**
         * 删除一个key
         */
        rocksDB.delete(key);
        System.out.println("after remove key:" + new String(key));
        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }
    }

    @Test
    public void testCreateColumnFamily() throws RocksDBException {
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor("my-first-columnfamily".getBytes(), cfOpts)
            );

            // a list which will hold the handles for the column families once the db is opened
            final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandleList)) {

                try {
                    // do something
                } finally {
                    // NOTE frees the column family handles before freeing the db
                    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
                        columnFamilyHandle.close();
                    }
                } // frees the db and the db options
            }
        } // frees the column family options

        Options options = new Options();
        options.setCreateIfMissing(true);
        List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);
        System.out.println("cfs.size: " + cfs.size());
        if (cfs.size() > 0) {
            for (byte[] cf : cfs) {
                System.out.println("ColumnFamily: " + new String(cf));
            }
        }
    }

    @Test
    public void testCertainColumnFamily() throws RocksDBException {
        String table = "CertainColumnFamilyTest";
        String key = "certainKey";
        String value = "certainValue";

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        Options options = new Options();
        options.setCreateIfMissing(true);

        List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);
        if (cfs.size() > 0) {
            for (byte[] cf : cfs) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
            }
        } else {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);

        rocksDB = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);
        for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
            if (new String(columnFamilyDescriptors.get(i).getName()).equals(table)) {
                rocksDB.dropColumnFamily(columnFamilyHandles.get(i));
            }
        }

        ColumnFamilyHandle columnFamilyHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(table.getBytes(), new ColumnFamilyOptions()));
        rocksDB.put(columnFamilyHandle, key.getBytes(), value.getBytes());

        byte[] getValue = rocksDB.get(columnFamilyHandle, key.getBytes());
        System.out.println("get value: " + new String(getValue));

        rocksDB.put(columnFamilyHandle, "SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key.getBytes());
        keys.add("SecondKey".getBytes());

        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        handleList.add(columnFamilyHandle);
        handleList.add(columnFamilyHandle);

        Map<byte[], byte[]> multiGet = rocksDB.multiGet(handleList, keys);
        for (Map.Entry<byte[],byte[]> entry : multiGet.entrySet()) {
            System.out.println(new String(entry.getKey()) + "--" + new String(entry.getValue()));
        }

        rocksDB.delete(columnFamilyHandle, key.getBytes());

        RocksIterator iter = rocksDB.newIterator(columnFamilyHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }
    }

}
