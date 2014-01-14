package com.netflix;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateColumnFamilyStatement;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pig.data.DataByteArray;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.aegisthus.tools.AegisthusSerializer;

public class KvsStrictMapper extends Mapper<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory
	    .getLogger(KvsStrictMapper.class);

    private static CFMetaData cfm;
    private static CFDefinition cfd;
    private static AbstractType keyConverter;
    private static AbstractType colConverter;
    private static AegisthusSerializer serializer = new AegisthusSerializer();
    private static JsonFactory jsonFactory = new JsonFactory();

    static {
	try {
	    String cql = "CREATE TABLE assess.kvs_strict ( part_key text, range_key text, version int, c_enc text, c_schema_ver text, content blob, is_deleted boolean, m_enc text, m_schema_ver text, metadata blob, modified timestamp, PRIMARY KEY ((part_key, range_key), version)) WITH CLUSTERING ORDER BY (version DESC) AND bloom_filter_fp_chance=0.010000 AND caching='KEYS_ONLY' AND comment='' AND dclocal_read_repair_chance=0.000000 AND gc_grace_seconds=864000 AND read_repair_chance=0.100000 AND replicate_on_write='true' AND populate_io_cache_on_flush='false' AND compaction={'class': 'SizeTieredCompactionStrategy'} AND compression={'sstable_compression': 'SnappyCompressor'};";
	    CreateColumnFamilyStatement statement = (CreateColumnFamilyStatement) QueryProcessor
		    .parseStatement(cql).prepare().statement;
	    cfm = new CFMetaData("assess", "kvs_strict",
		    ColumnFamilyType.Standard, statement.comparator, null);
	    statement.applyPropertiesTo(cfm);

	    cfd = cfm.getCfDef();
	    LOG.info("cf def " + cfd);
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }

    KvsStrictMapper() {
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void map(Text key, Text value, Context context)
	    throws IOException, InterruptedException {
	Map<String, Object> data = serializer.deserialize(value.toString());

	DataByteArray rowKey = (DataByteArray) data
		.remove(AegisthusSerializer.KEY);
	data.remove(AegisthusSerializer.DELETEDAT);

	String currentVersion = null;
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	JsonGenerator jsonGen = jsonFactory.createJsonGenerator(out,
		JsonEncoding.UTF8);

	jsonGen.writeStartObject();
	
	for (Object colObj : data.values()) {
	    List<Object> col = (List<Object>) colObj;

	    if (col.size() > 3)
		continue;

	    String name = (String) col.get(0);
	    DataByteArray colValue = (DataByteArray) col.get(1);

	    // kvs3 names are of the form version:<column name>
	    String[] nameParts = name.split(":");

	    if (nameParts.length == 1)
		continue; // CQL3 includes "<version>:" columns for some reason
	    if (nameParts.length != 2) {
		LOG.warn("couldn't parse name: " + name);
	    }

	    String version = nameParts[0];
	    String colName = nameParts[1];

	    if (currentVersion == null) {
		currentVersion = version;
	    } else if (!currentVersion.equals(version)) {
		if (Integer.parseInt(currentVersion) < Integer.parseInt(version)) {
		    out.close();
		    out = new ByteArrayOutputStream();
		    jsonGen = jsonFactory.createJsonGenerator(out, JsonEncoding.UTF8);
		    jsonGen.writeStartObject();
		    currentVersion = version;
		}
	    }

	    writeColumn(jsonGen, colName, colValue);
	}

	jsonGen.writeEndObject();
	jsonGen.flush();

	if (currentVersion != null) {
	    context.write(new Text(rowKey.toString() + "@" + currentVersion),
		    new Text(out.toByteArray()));
	}
	out.close();
    }

    private void writeColumn(JsonGenerator json, String colName, DataByteArray colData) throws JsonGenerationException, IOException {
	ByteBuffer wrappedColData = BytesType.instance.fromString(colData.toString());
	ColumnIdentifier colId = new ColumnIdentifier(colName, false);
	CFDefinition.Name name = cfd.get(colId);
	AbstractType<?> type = name.type;
	
	json.writeFieldName(colName);

	if (type instanceof Int32Type) {
	    json.writeNumber(Int32Type.instance.compose(wrappedColData));
	} else if (type instanceof UTF8Type) {
	    json.writeString(UTF8Type.instance.compose(wrappedColData));
	} else if (type instanceof DateType) {
	    Date date = DateType.instance.compose(wrappedColData);
	    json.writeNumber(date.getTime());
	} else if (type instanceof BooleanType) {
	    json.writeBoolean(BooleanType.instance.compose(wrappedColData));
	} else {
	    // assume all is string for now.
	    json.writeString(UTF8Type.instance.compose(wrappedColData));
	    //json.writeString(colData.toString());
	}
    }

}
