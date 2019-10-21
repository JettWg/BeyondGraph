package org.beyond.graph.bulk.loading;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class Remote {
    private static final Logger LOG = LoggerFactory.getLogger(Remote.class);

    private Cluster cluster;
    private Client client;

    private static Remote instance = null;

    private synchronized void initialize() {
        if (null != instance) {
            final String errMsg = "You may not instatiate a Remote";
            throw new RuntimeException(errMsg);
        }
        instance = this;
    }

    public Remote(String remoteConfigFile) throws Exception {
        initialize();
        cluster = Cluster.open(remoteConfigFile);
        client = cluster.connect();
    }

    public static void connect(String remoteConfigFile) throws Exception {
        new Remote(remoteConfigFile);
    }

    public static Remote getInstance() {
        if (null == instance) {
            final String errMsg = "You may not instatiate a Remote";
            throw new RuntimeException(errMsg);
        }
        return instance;
    }

    public void close() {
        LOG.info("*** Close Remote Connect ***");
        client.close();
        cluster.close();
    }

    public Set<String> getGraphNameSet() {
        LOG.info("*** Get Remote Opened Graph Names ***");
        final StringBuilder s = new StringBuilder();
        s.append("ConfiguredGraphFactory.getGraphNames();");
        ResultSet resultSet = client.submit(s.toString());
        return resultSet.stream().map(Result::getString).collect(Collectors.toSet());
    }

    public void openGraph(String graphName, Configuration conf) {
        LOG.info("*** Open Remote Graph ***");
        final StringBuilder s = new StringBuilder();
        // config storage
        s.append("Map<String, Object> map = new HashMap<>(); ");
        s.append(String.format("map.put(\"storage.backend\", \"%s\"); ", conf.getString("storage.backend")));
        s.append(String.format("map.put(\"storage.hostname\", \"%s\"); ", conf.getString("storage.hostname")));
        // config index
        s.append(String.format("map.put(\"index.search.backend\", \"%s\"); ", conf.getString("index.search.backend")));
        s.append(String.format("map.put(\"index.search.hostname\", \"%s\"); ", conf.getString("index.search.hostname")));
        s.append(String.format("map.put(\"index.search.port\", \"%s\"); ", conf.getString("index.search.port")));
        // config table name
        s.append(String.format("map.put(\"storage.hbase.table\", \"%s\"); ", conf.getString("storage.hbase.table")));
        s.append(String.format("map.put(\"index.search.index-name\", \"%s\"); ", conf.getString("index.search.index-name")));
        // config extra items
        s.append("map.put(\"ids.block-size\", 100000); ");
        s.append("map.put(\"storage.batch-loading\", true); ");
        s.append("map.put(\"graph.allow-upgrade\", true); ");
        // config graphname
        s.append(String.format("map.put(\"graph.graphname\", \"%s\"); ", graphName));
        s.append("ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map)); ");
        // open graph
        s.append(String.format("ConfiguredGraphFactory.open(\"%s\"); ", graphName));

        client.submit(s.toString());
    }

    public void closeGraph(String graphName) {
        LOG.info("*** Close Remote Graph ***");
        final StringBuilder s = new StringBuilder();
        s.append(String.format("ConfiguredGraphFactory.close(\"%s\"); ", graphName));
        client.submit(s.toString());
    }

    public void dropGraph(String graphName) {
        LOG.info("*** Drop Remote Graph ***");
        final StringBuilder s = new StringBuilder();
        s.append(String.format("ConfiguredGraphFactory.drop(\"%s\"); ", graphName));
        client.submit(s.toString());
    }

    public void removeConfiguration(String graphName) {
        LOG.info("*** Remove Remote Graph Configuration ***");
        final StringBuilder s = new StringBuilder();
        s.append(String.format("ConfiguredGraphFactory.removeConfiguration(\"%s\"); ", graphName));
        client.submit(s.toString());
    }

}
