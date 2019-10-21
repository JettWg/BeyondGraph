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
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.beyond.graph.bulk.loading.schema.SchemaBuilder;
import org.beyond.graph.bulk.loading.storage.KVStorage;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Transaction;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class EdgeFileHandler extends AbstractElementFileHandler implements Closeable, AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(EdgeFileHandler.class);
	private int limitRows;
	private int batchSize;
	private boolean ignoreMissingNodes;
	
	public EdgeFileHandler(String files, KVStorage kvStorage, int limitRows, int batchSize, boolean ignoreMissingNodes) throws FileNotFoundException {
		super(files, kvStorage);
		this.limitRows = limitRows;
		this.batchSize = batchSize;
		this.ignoreMissingNodes = ignoreMissingNodes;
	}

	public void insertContent(JanusGraph mainGraph) {
		int edgesCreated = 0;
		int startIDColumn = findTag(ColumnHandler.Tag.START_ID);
		int startLabelColumn = findTag(ColumnHandler.Tag.START_LABEL);
		int endIDColumn = findTag(ColumnHandler.Tag.END_ID);
		int endLabelColumn = findTag(ColumnHandler.Tag.END_LABEL);
		int typeColumn = findTag(ColumnHandler.Tag.TYPE);

		if (startIDColumn == -1) {
			throw new RuntimeException("No start-id-column for relationship");
		}
		if (startLabelColumn == -1) {
			throw new RuntimeException("No start-label-column for relationship");
		}
		if (endIDColumn == -1) {
			throw new RuntimeException("No end-id-column for relationship");
		}
		if (endLabelColumn == -1) {
			throw new RuntimeException("No end-label-column for relationship");
		}
		if (typeColumn == -1) {
			throw new RuntimeException("No type column for relationship");
		}
		ColumnHandler startIDHandler = columns[startIDColumn];
		ColumnHandler startLabelHandler = columns[startLabelColumn];
		ColumnHandler endIDHandler = columns[endIDColumn];
		ColumnHandler endLabelHandler = columns[endLabelColumn];
		ColumnHandler typeHandler = columns[typeColumn];
		
		StopWatch watch = new StopWatch();
		watch.start();
		Transaction graph = mainGraph.newTransaction();
		try {
			do  {
				if (this.currentParser == null) {
						setupCSVParser(false);
				}
				for (CSVRecord record : currentParser) {
					if (edgesCreated >= limitRows) break;
					int columns = Math.min(record.size(), this.columns.length);
					
					Object startId = startIDHandler.convert(record.get(startIDColumn));
					if (startId == null) {
						LOG.debug("Start-id field of edge record #{} of {} missing", currentParser.getRecordNumber(), currentFile);
						continue;
					}
					String startLabel = (String) startLabelHandler.convert(record.get(startLabelColumn));
					if (startLabel == null) {
						LOG.debug("Start-label field of edge record #{} of {} missing", currentParser.getRecordNumber(), currentFile);
						continue;
					}
					Object endId = endIDHandler.convert(record.get(endIDColumn));
					if (endId == null) {
						LOG.debug("End-id field of edge record #{} of {} missing -- skipping", currentParser.getRecordNumber(), currentFile);
						continue;
					}
					String endLabel = (String) endLabelHandler.convert(record.get(endLabelColumn));
					if (endLabel == null) {
						LOG.debug("End-label field of edge record #{} of {} missing", currentParser.getRecordNumber(), currentFile);
						continue;
					}

					Object janusStartKey = kvStorage.get(startLabel, startId.toString());
					Object janusEndKey = kvStorage.get(endLabel, endId.toString());
					
					if (janusStartKey == null) {
						LOG.debug("Making Edge from {}, but vertex wasn't created", startId);
						if (! ignoreMissingNodes) {
							LOG.error("Making Edge from {}, but vertex wasn't created -- aborting", startId);
							break;
						}
						continue;
					}
					if (janusEndKey == null) {
						LOG.debug("Making Edge to {}, but vertex wasn't created", endId);
						if (! ignoreMissingNodes) {
							LOG.error("Making Edge to {}, but vertex wasn't created -- aborting", endId);
							break;
						}
						continue;
					}

					Iterator<Vertex> vertices = graph.vertices(janusStartKey, janusEndKey);
					if (! vertices.hasNext()) {
						LOG.warn("Vertex with id {} (graph id {}) couldn't be found -- skipping", startId, janusStartKey);
						continue;
					}
					Vertex fromVertex = vertices.next();
					if (! vertices.hasNext()) {
						LOG.warn("Vertex with id {} (graph id {}) couldn't be found -- skipping", endId, janusEndKey);
						continue;
					}
					Vertex toVertex = vertices.next();
					String typeName = (String)typeHandler.convert(record.get(typeColumn));
					Edge addedEdge;

					if (fromVertex.id().toString().equals(janusStartKey)) {
						addedEdge = fromVertex.addEdge(typeName, toVertex);
					} else if (toVertex.id().toString().equals(janusStartKey)) {
						addedEdge = toVertex.addEdge(typeName, fromVertex);
					} else {
						LOG.warn("Vertex with id {}->{} (graph id {}->{}) couldn't be found -- skipping",
								startId, endId, janusStartKey, janusEndKey);
						continue;
					}

					addedEdge.property("_label", typeName);
					
					for (int c = 0; c < columns; ++c) {
						ColumnHandler handler = this.columns[c];
						ColumnHandler.Tag tag = handler.getTag();
						if (tag == ColumnHandler.Tag.IGNORE
								|| tag == ColumnHandler.Tag.START_ID || tag == ColumnHandler.Tag.START_LABEL
								|| tag == ColumnHandler.Tag.END_ID || tag == ColumnHandler.Tag.END_LABEL
								|| tag == ColumnHandler.Tag.TYPE) continue;
						
						Object value = handler.convert(Strings.emptyToNull(record.get(c)));
						if (value != null) {
							addedEdge.property(handler.getName(), value);
						}
					}
					if (++edgesCreated % 10000 == 0) {
						graph.tx().commit();
						graph.tx().close();
						graph.close();
						graph = mainGraph.newTransaction();
						LOG.info("Created {} edges in {} ms, {} ms/edge", edgesCreated, watch.getTime(), (double) watch.getTime() / edgesCreated); 
					}
				}
				close();
			} while (! files.isEmpty());
			graph.tx().commit();
			graph.tx().close();
			graph.close();
			
			LOG.info("Created {} edges in {} ms, {} ms/edge", edgesCreated, watch.getTime(), (edgesCreated > 0 ? ((double) watch.getTime() / edgesCreated) : Double.NaN)); 
		} catch (IOException | RocksDBException e) {
			LOG.error("Error reading file or writing vertex", e);
		}
	}
	
	private int findTag(ColumnHandler.Tag tag) {
		for (int i = 0 ; i < columns.length; ++i) {
			if (columns[i].getTag() == tag) return i;
		}
		return -1;
	}

	public void parseHeaders(SchemaBuilder schemaBuilder) throws IOException {
		setupCSVParser(true);
		int maxColumn = currentParser.getHeaderMap().values().stream().mapToInt(Integer::intValue).max().getAsInt();
		columns = new ColumnHandler[maxColumn+1];
		
		this.currentParser.getHeaderMap().forEach((label, index) -> {
			ColumnHandler handler = makeColumnHandler(label);
			columns[index] = handler;
			
			ColumnHandler.Tag tag = handler.getTag();
			if (! handler.getName().equals("")
					&& tag != ColumnHandler.Tag.END_ID
					&& tag != ColumnHandler.Tag.END_LABEL
					&& tag != ColumnHandler.Tag.START_ID
					&& tag != ColumnHandler.Tag.START_LABEL
				    && tag != ColumnHandler.Tag.TYPE
				    && tag != ColumnHandler.Tag.IGNORE) {
				schemaBuilder.property(handler.getName(), handler.getDatatype());
			}
		});
	}
	
	public String getDescription() {
		return String.join(", ", files.stream().map(Object::toString).collect(Collectors.toList()));
	}
	
}
