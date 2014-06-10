/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.aegisthus.io.sstable;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.hamcrest.core.IsInstanceOf;

import com.google.common.collect.Maps;

/**
 * This class serializes each row in a SSTable to a string.
 * 
 * <pre>
 * SSTableScanner scanner = new SSTableScanner(&quot;columnfamily-g-1-Data.db&quot;);
 * while (scanner.hasNext()) {
 * 	String row = scanner.next();
 * 	System.out.println(jsonRow);
 * }
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class SSTableScanner extends SSTableReader implements Iterator<String> {
	private static final Log LOG = LogFactory.getLog(SSTableScanner.class);

	public static final String COLUMN_NAME_KEY = "$$CNK$$";
	public static final String KEY = "$$KEY$$";
	public static final String SUB_KEY = "$$SUB_KEY$$";

	private AbstractType columnNameConvertor = null;

	private Map<String, AbstractType> converters = Maps.newHashMap();
	private AbstractType keyConvertor = null;
	private boolean promotedIndex = true;
	private final OnDiskAtom.Serializer serializer = OnDiskAtom.Serializer.instance;
  private Descriptor.Version version = null;


	public SSTableScanner(DataInput input, Descriptor.Version version) {
		this(input, null, -1, version);
	}

	public SSTableScanner(DataInput input, Map<String, AbstractType> converters, Descriptor.Version version) {
		this(input, converters, -1, version);
	}

	public SSTableScanner(DataInput input, Map<String, AbstractType> converters, long end, Descriptor.Version version) {
		init(input, converters, end);
		this.version = version;
	}

	public SSTableScanner(String filename) {
		this(filename, null, 0, -1);
	}

	public SSTableScanner(String filename, long start) {
		this(filename, null, start, -1);
	}

	public SSTableScanner(String filename, long start, long end) {
		this(filename, null, start, end);
	}

	public SSTableScanner(String filename, Map<String, AbstractType> converters, long start) {
		this(filename, converters, start, -1);
	}

	public SSTableScanner(String filename, Map<String, AbstractType> converters, long start, long end) {
		try {
			this.version = Descriptor.fromFilename(filename).version;
			init(	new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 65536 * 10)),
					converters,
					end);
			if (start != 0) {
				skipUnsafe(start);
				this.pos = start;
			}
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	public void close() {
		if (input != null) {
			try {
				((DataInputStream) input).close();
			} catch (IOException e) {
				// ignore
			}
			input = null;
		}
	}

	private AbstractType getConvertor(String convertor) {
		return getConvertor(convertor, BytesType.instance);
	}

	private AbstractType getConvertor(String convertor, AbstractType defaultType) {
		if (converters != null && converters.containsKey(convertor)) {
			return converters.get(convertor);
		}
		return defaultType;
	}

	public long getDatasize() {
		return datasize;
	}

	@Override
	public boolean hasNext() {
		return hasMore();
	}

	protected void init(DataInput input, Map<String, AbstractType> converters, long end) {
		this.input = input;
		if (converters != null) {
			this.converters = converters;
		}
		this.end = end;

		this.columnNameConvertor = getConvertor(COLUMN_NAME_KEY);
		this.keyConvertor = getConvertor(KEY);
	}

	protected void insertKey(StringBuilder bf, String value) {
		bf.append("\"").append(value).append("\": ");
	}

	/**
	 * TODO: Change this to use AegisthusSerializer
	 * 
	 * Returns json for the next row in the sstable. <br/>
	 * <br/>
	 * 
	 * <pre>
	 * ColumnFamily:
	 * {key: {columns: [[col1], ... [colN]], deletedAt: timestamp}}
	 * </pre>
	 */
	@Override
	public String next() {
		StringBuilder str = new StringBuilder();
		try {
			int keysize = input.readUnsignedShort();
			byte[] b = new byte[keysize];
			input.readFully(b);
			String key = getSanitizedName(ByteBuffer.wrap(b), keyConvertor);

      long rowSize = Long.MAX_VALUE;
      if (version.hasRowSizeAndColumnCount)
          rowSize = input.readLong();

			datasize = keysize + 2 // keysize byte
        + 4 // local deletion time
        + 8; // marked for delete
			if (!promotedIndex) {
				if (input instanceof DataInputStream) {
					// skip bloom filter
					skip(input.readInt());
					// skip index
					skip(input.readInt());
				} else {
					// skip bloom filter
					int bfsize = input.readInt();
					input.skipBytes(bfsize);
					// skip index
					int idxsize = input.readInt();
					input.skipBytes(idxsize);
				}
			}
			// The local deletion times are similar to the times that they were
			// marked for delete, but we only
			// care to know that it was deleted at all, so we will go with the
			// long value as the timestamps for
			// update are long as well.
			@SuppressWarnings("unused")
			int localDeletionTime = input.readInt();
			long markedForDeleteAt = input.readLong();

      int columnCount = Integer.MAX_VALUE;
      if (version.hasRowSizeAndColumnCount)
          columnCount = input.readInt();

			str.append("{");
			insertKey(str, key);
			str.append("{");
			insertKey(str, "deletedAt");
			str.append(markedForDeleteAt);
			str.append(", ");
			insertKey(str, "columns");
			str.append("[");
			if (datasize > 100000000) {
			  LOG.info("Skipping row [" + key + "] with too much data: " + datasize);
                          serializeColumns(str, columnCount, input, true);
			} else {
                          serializeColumns(str, columnCount, input, false);
			}
			str.append("]");
			str.append("}}\n");
		} catch (IOException e) {
			throw new IOError(e);
		}

    this.pos += datasize;

		return str.toString();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void serializeColumns(StringBuilder sb, int count, DataInput columns) throws IOException {
	  serializeColumns(sb, count, columns, false);
	}

	public void serializeColumns(StringBuilder sb, int count, DataInput columns, Boolean skip) throws IOException {
    int initialLength = sb.length();

    boolean moreThanOne = false;

		for (int i = 0; i < count; i++) {
			// serialize columns
			OnDiskAtom atom = serializer.deserializeFromSSTable(columns, this.version);

      if (atom == null) {
        this.datasize += 2;   // END_OF_ROW is 2 bytes
        break;
      }

      long atomSize = atom.serializedSizeForSSTable();
      this.datasize += atomSize;

      if (!skip && datasize > 100000000) {
        skip = true;
        sb.delete(initialLength, sb.length());
      }

			if (skip) continue;
			if (atom instanceof Column) {
          if (moreThanOne) {
              sb.append(", ");
          } else {
            moreThanOne = true;
          }

			    Column column = (Column) atom;
                String cn = getSanitizedName(column.name(), columnNameConvertor);
                sb.append("[\"");
                sb.append(cn);
                sb.append("\", \"");
                sb.append(getConvertor(cn).getString(column.value()));
                sb.append("\", ");
                sb.append(column.timestamp());

                if (column instanceof DeletedColumn) {
                  sb.append(", ");
                  sb.append("\"d\"");
                } else if (column instanceof ExpiringColumn) {
                  sb.append(", ");
                  sb.append("\"e\"");
                  sb.append(", ");
                  sb.append(((ExpiringColumn) column).getTimeToLive());
                  sb.append(", ");
                  sb.append(column.getLocalDeletionTime());
                } else if (column instanceof CounterColumn) {
                  sb.append(", ");
                  sb.append("\"c\"");
                  sb.append(", ");
                  sb.append(((CounterColumn) column).timestampOfLastDelete());
                }
                sb.append("]");
			} else if (atom instanceof RangeTombstone){
			    RangeTombstone tomb = (RangeTombstone) atom;
			    LOG.info("Found range tombstone! " + tomb.toString());
			}
		}
	}
	
	private String getSanitizedName(ByteBuffer name, AbstractType convertor) {
	    return convertor.getString(name)
                    .replaceAll("[\\s\\p{Cntrl}]", " ")
                    .replace("\\", "\\\\");
	}
}
