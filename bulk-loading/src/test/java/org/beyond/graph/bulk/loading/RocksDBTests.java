package org.beyond.graph.bulk.loading;

import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RocksDBTests {
    private static final String dbPath = "./kv_db/";

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



}
