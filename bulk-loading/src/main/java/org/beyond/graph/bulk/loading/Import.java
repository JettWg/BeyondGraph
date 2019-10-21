// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.beyond.graph.bulk.loading;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.beyond.graph.bulk.loading.schema.DefaultSchemaBuilder;
import org.beyond.graph.bulk.loading.storage.KVStorage;
import org.beyond.graph.bulk.loading.utils.IOConsumer;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.Help;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class Import implements Callable<Void> {
	private static final Logger LOG = LoggerFactory.getLogger(Import.class);

	@Option(names = {"--graph-name"})
	private String graphName = "graph_" + (new SimpleDateFormat("yyyymmdd")).format(new Date());

	@Option(names = {"-r", "--remote"}, required = true)
	private String remoteConfigFile;

	@Option(names = {"-c", "--config"}, required = true)
	private String configFile;

	@Option(names = {"--kv-storage-dir"})
	private String kvStorageDir = "./kv_data";

	@Option(names = {"--offline-import"})
	private boolean offline = false;

	@Option(names = {"--ignore-missing-nodes"})
    private boolean ignoreMissingNodes = true;

	@Option(names = {"--add-label-property"})
    private boolean addLabelProperty = true;

	@Option(names = {"-n", "--limit-rows"})
    private int limitRows = -1;

	@Option(names = {"--batch-size"})
	private int batchSize = 10000;

    @Option(names = {"-D", "--drop-before-import"})
    private boolean drop = false;

    @Option(names = {"--threads"}, description = "Number of threads to run concurrently")
    private int poolSize = 2;
    
    @Option(names = {"-i", "--index"}, split=",")
    private Set<String> index = new LinkedHashSet<>();

    @Option(names = {"-g", "--global-index"}, split=",")
    private Set<String> globalIndex = new LinkedHashSet<>();

    @Option(names = {"--edgeLabels"}, split=",")
    private List<String> edgeLabels = new LinkedList<>();

    @Option(names = {"--nodes"}, required=true)
    private Map<String, String> nodes = new LinkedHashMap<>();
    
    @Option(names = {"--relationships"})
    private List<String> relationships = new LinkedList<>();

    private boolean isGraphExisted = false;
    private Set<String> nodeLabels = new HashSet<>();

	@Override
	public Void call() throws Exception {

		if (limitRows < 0) limitRows = Integer.MAX_VALUE-1;

		List<VertexFileHandler> vertexHandlers = new LinkedList<>();
		List<EdgeFileHandler> edgeHandlers = new LinkedList<>();
		
		try {
			// 获取要导入的nodes的label列表信息
			nodeLabels = new HashSet<>(nodes.keySet());

			// 初始化节点的id-mapping存储kv引擎
			KVStorage.initialize(kvStorageDir, graphName, nodeLabels);

			if (!offline) {
				// 建立与远程服务的连接，并检查当前操作的图是否已存在
				Remote.connect(remoteConfigFile);
				Set<String> graphNameSet = Remote.getInstance().getGraphNameSet();
				isGraphExisted = graphNameSet.contains(graphName);
				LOG.info("*** Graph: {} in {} = {}", graphName, graphNameSet, isGraphExisted);
			}

			// Open the vertex headers
			for (Map.Entry<String, String> entry : nodes.entrySet()) {
				VertexFileHandler handler = new VertexFileHandler(entry.getKey(), entry.getValue(), KVStorage.getInstance(), limitRows, batchSize);
				vertexHandlers.add(handler);
			}

			// Open the edge headers
			for (String files : relationships) {
				EdgeFileHandler handler = new EdgeFileHandler(files, KVStorage.getInstance(), limitRows, batchSize, ignoreMissingNodes);
				edgeHandlers.add(handler);
			}

			try(JanusGraph graph = initializeGraph()) {
		
				LOG.info("*** Building schema ***");
				try(DefaultSchemaBuilder schema = new DefaultSchemaBuilder(graph)) {
					forEach(vertexHandlers, handler -> handler.parseHeaders(schema));
					forEach(edgeHandlers, handler -> handler.parseHeaders(schema));
					
					forEach(edgeLabels, label -> schema.edge(label.trim()).build());
					schema.globalVertexIndex("_label", String.class);
					schema.done();
				}
				
				LOG.info("*** Creating vertices ***");
				doWithExecutor(executor -> {
					forEach(vertexHandlers, (h) -> {
						executor.execute(() -> {
							LOG.info("Starting to write vertices: {}", h.getVertexLabelName());
							try {
								h.insertContent(graph);
								LOG.info("Done writing vertices: {}", h.getVertexLabelName());
							} catch (Exception e) {
								LOG.error("Error handling vertex label: " + h.getVertexLabelName(), e);
							}
						});
					});
				});
				
				LOG.info("*** Creating edge ***");
				doWithExecutor(executor -> {
					forEach(edgeHandlers, (h) -> {
						executor.execute(() -> {
							String desc = h.getDescription();
							LOG.info("Starting to write edges from: {}", desc);
							try {
								h.insertContent(graph);
								LOG.info("Starting to write edges from: {}", desc);
							} catch (Exception e) {
								LOG.error("Error handling vertex label: " + desc, e);
							}
						});
					});
				});
			}

			if (!offline) {
				// 如果图不存在系统中 或者 已经删除了，则重新配置并打开图，否则不做额外的打开操作
				if (!isGraphExisted || drop) {
					// 配置远程服务的图信息并打开该图
					LOG.info("Configure Remote Graph Mapping and Open Graph");
					configRemoteAndOpenGraph();
				}
			}
		} finally {
			LOG.info("Closing handlers");
			forEach(vertexHandlers, Closeable::close);
			forEach(edgeHandlers, Closeable::close);
			// close remote connection
			if (!offline) {
				Remote.getInstance().close();
			}
			// close kv-storage's connection
			KVStorage.getInstance().close();
		}
		LOG.info("Done importing");
        return null;
	}

	private JanusGraph initializeGraph() throws BackendException, ConfigurationException, RocksDBException {
		LOG.info("Opening graph from information in {}", configFile);

		Configuration conf = getConfiguration(configFile);
		// drop graph
		if (drop) {
			LOG.info("DROPPING GRAPH AT {}!", configFile);
			// remove remote graph
			if (!offline && isGraphExisted) {
				Remote.getInstance().dropGraph(graphName);
			}
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// remove graph data table in storage
			JanusGraph graph = JanusGraphFactory.open(conf);
			JanusGraphFactory.drop(graph);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// remove kv-storage's id-mappings
			KVStorage.getInstance().drop();
		}
		// re-open kv-storage-engine
		KVStorage.getInstance().open(kvStorageDir, graphName, nodeLabels);
		JanusGraph graph = JanusGraphFactory.open(configFile);
		return graph;
	}

	private Configuration getConfiguration(String configFile) throws ConfigurationException {
		Configuration conf = new PropertiesConfiguration(configFile);
		conf.setProperty("storage.hbase.table", "gtbl_" + graphName);
		conf.setProperty("index.search.index-name", "gtbl_" + graphName);
		return conf;
	}

	private void configRemoteAndOpenGraph() throws ConfigurationException {
		Configuration conf = getConfiguration(configFile);
		Remote.getInstance().openGraph(graphName, conf);
	}

	private <T> void forEach(Iterable<T> handlers, IOConsumer<T> consumer) throws IOException {
		for (T vfe : handlers) {
			consumer.accept(vfe);
		}
	}

	void doWithExecutor(IOConsumer<ExecutorService> consumer) throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(poolSize);
		consumer.accept(executor);
		LOG.info("Awaiting termination of jobs");
		awaitTerminationAfterShutdown(executor);
	}	
	
	public static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
	    threadPool.shutdown();
	    try {
	        if (!threadPool.awaitTermination(48, TimeUnit.HOURS)) {
	            threadPool.shutdownNow();
	        }
	    } catch (InterruptedException ex) {
	        threadPool.shutdownNow();
	        // We've been asked to shut down, maybe Ctrl+C
	        try {
				threadPool.awaitTermination(120, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
		        Thread.currentThread().interrupt();
		        return;
			}
	        Thread.currentThread().interrupt();
	    }
	}
	
	public static void main(String[] args) {
		new CommandLine(new Import())
			.setCaseInsensitiveEnumValuesAllowed(true)
			.parseWithHandlers(
				new RunLast()
					.useOut(System.out)
					.useAnsi(Help.Ansi.AUTO),
					new DefaultExceptionHandler<List<Object>>()
						.useErr(System.err)
						.useAnsi(Help.Ansi.AUTO),
				args);
	}
}	

