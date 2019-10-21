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

import com.google.common.base.Strings;
import org.beyond.graph.bulk.loading.ColumnHandler.Tag;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.beyond.graph.bulk.loading.schema.SchemaBuilder;
import org.beyond.graph.bulk.loading.schema.VertexTypeBuilder;
import org.beyond.graph.bulk.loading.storage.KVStorage;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Transaction;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class VertexFileHandler extends AbstractElementFileHandler implements Closeable, AutoCloseable {
	final String vertexLabelName;
	
	private static final Logger LOG = LoggerFactory.getLogger(VertexFileHandler.class);

	private int limitRows;
	private int batchSize;
	public VertexFileHandler(String vertexLabelName, String files, KVStorage kvStorage, int limitRows, int batchSize) throws FileNotFoundException {
		super(files, kvStorage);
		this.vertexLabelName = vertexLabelName;
		this.limitRows = limitRows;
		this.batchSize = batchSize;
	}

	public String getVertexLabelName() {
		return vertexLabelName;
	}

	private void batchInsertContent(Transaction tx, Map<String, CSVRecord> batchRecords) throws RocksDBException {
		// 对于该批次的信息查询db， 获取映射的id值
		Map<String, String> idMappings = kvStorage.multiGets(vertexLabelName, new ArrayList<>(batchRecords.keySet()));

		// 对已有的跳过处理，不存在的插入到db中
		Map<String, String> newVertexIDMappings = new HashMap<>();
		for (Map.Entry<String, CSVRecord> rkv : batchRecords.entrySet()) {
			CSVRecord record = rkv.getValue();
			int recordCols = Math.min(record.size(), this.columns.length);
			JanusGraphVertex addedVertex;

			String innerID = idMappings.getOrDefault(rkv.getKey(), null);
			if (null == innerID) {
				// 节点不存在，插入新节点，并将id-mapping关系存入db
				addedVertex = tx.addVertex(vertexLabelName);
				addedVertex.property("_label", vertexLabelName);
				// fixme 根据id()的实际类型转换为String
				newVertexIDMappings.put(rkv.getKey(), addedVertex.id().toString());
			} else {
				// 节点已经存在，只更新属性
				Iterator<Vertex> vertices = tx.vertices(innerID);
				if (!vertices.hasNext()) {
					LOG.warn("Vertex with id {} (graph id {}) couldn't be found -- skipping", rkv.getKey(), innerID);
					continue;
				}
				addedVertex = (JanusGraphVertex) vertices.next();
			}

			for (int c = 0; c < recordCols; c++) {
				ColumnHandler handler = this.columns[c];
				Tag tag = handler.getTag();
				if (tag == Tag.IGNORE) continue;

				Object colValue = handler.convert(Strings.emptyToNull(record.get(c)));
				if (colValue != null) {
					addedVertex.property(handler.getName(), colValue);
				}
			}
		}

		// 不存在的节点插入到db中
		kvStorage.multiSets(vertexLabelName, newVertexIDMappings);
	}

	public void insertContent(JanusGraph mainGraph) {
		int verticesCreated = 0;
		StopWatch watch = new StopWatch();
		watch.start();
		Transaction graph = mainGraph.newTransaction();
		try {
			// 记录一个批次的数据
			Map<String, CSVRecord> batchRecords = new HashMap<>();
			do  {
				if (this.currentParser == null) {
						setupCSVParser(false);
				}

				for (CSVRecord record : currentParser) {
					if (verticesCreated >= limitRows) break;

					// 记录一个批次的id:record（该批次内重复的，以最后出现的记录为准，覆盖前面已有的key）

					int idColumn = findIDTag();
					if (idColumn == -1) {
						throw new RuntimeException("No id-column for vertex");
					}
					ColumnHandler idHandler = columns[idColumn];
					Object idValue = idHandler.convert(record.get(idColumn));
					if (idValue == null) {
						LOG.debug("id field of vertex record #{} of {} missing -- skipping", currentParser.getRecordNumber(), currentFile);
						continue;
					}

					batchRecords.put(idValue.toString(), record);

					if (++verticesCreated % batchSize == 0) {
						// 处理该批次的数据
						batchInsertContent(graph, batchRecords);

						// clear batchRecords, continue next batch
						batchRecords.clear();

						graph.tx().commit();
						graph.tx().close();
						graph.close();
						graph = mainGraph.newTransaction();
						LOG.info("Created {} {} vertices in {} ms, {} ms/vertex", verticesCreated, vertexLabelName, watch.getTime(), (double) watch.getTime() / verticesCreated);
					}
				}
				close();
			} while (! files.isEmpty());
			// 处理最后一个批次的数据
			batchInsertContent(graph, batchRecords);

			graph.tx().commit();
			graph.tx().close();
			graph.close();
			LOG.info("Created {} {} vertices in {} ms, {} ms/vertex", verticesCreated, vertexLabelName, watch.getTime(), verticesCreated > 0 ? (double) watch.getTime() / verticesCreated : Double.NaN); 
		} catch (IOException | RocksDBException e) {
			LOG.error("Error reading file or writing vertex", e);
		}
	}

	private int findIDTag() {
		for (int i = 0 ; i < columns.length; ++i) {
			if (columns[i].getTag() == ColumnHandler.Tag.ID) return i;
		}
		return -1;
	}

	public void parseHeaders(SchemaBuilder schemaBuilder) throws IOException {
		setupCSVParser(true);
		int maxColumn = currentParser.getHeaderMap().values().stream().mapToInt(Integer::intValue).max().getAsInt();
		columns = new ColumnHandler[maxColumn+1];
		VertexTypeBuilder vertexBuilder = schemaBuilder.vertex(vertexLabelName);
		
		this.currentParser.getHeaderMap().forEach((label, index) -> {
			ColumnHandler handler = makeColumnHandler(label);
			columns[index] = handler;
			
			// Now create the property
			if (handler.getTag() == ColumnHandler.Tag.ID || handler.getTag() == ColumnHandler.Tag.UNIQUE) {
				vertexBuilder.key(handler.getName(), handler.getDatatype());
			} else if (handler.getTag() == ColumnHandler.Tag.INDEX) {
				vertexBuilder.indexedProperty(handler.getName(), handler.getDatatype());
			} else if (handler.getTag() == ColumnHandler.Tag.DATA) {
				vertexBuilder.property(handler.getName(), handler.getDatatype());
			}
		});
		vertexBuilder.build();
	}
	public String getName() {
		return vertexLabelName;
	}
	
}
