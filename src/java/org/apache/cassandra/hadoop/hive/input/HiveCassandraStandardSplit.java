/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop.hive.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

@SuppressWarnings("deprecation")
public class HiveCassandraStandardSplit extends FileSplit implements InputSplit
{
    private final ColumnFamilySplit split;
    private String columnMapping;
    private String keyspace;
    private String columnFamily;
    private int rangeBatchSize;
    private int slicePredicateSize;
    private int splitSize;
    // added for 7.0
    private String partitioner;
    private int port;
    private String host;

    public HiveCassandraStandardSplit()
    {
        super((Path) null, 0, 0, (String[]) null);
        columnMapping = "";
        split = new ColumnFamilySplit(null, null, null);
    }

    public HiveCassandraStandardSplit(ColumnFamilySplit split, String columnsMapping, Path dummyPath)
    {
        super(dummyPath, 0, 0, (String[]) null);
        this.split = split;
        columnMapping = columnsMapping;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        super.readFields(in);
        columnMapping = in.readUTF();
        keyspace = in.readUTF();
        columnFamily = in.readUTF();
        rangeBatchSize = in.readInt();
        slicePredicateSize = in.readInt();
        partitioner = in.readUTF();
        port = in.readInt();
        host = in.readUTF();
        split.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        super.write(out);
        out.writeUTF(columnMapping);
        out.writeUTF(keyspace);
        out.writeUTF(columnFamily);
        out.writeInt(rangeBatchSize);
        out.writeInt(slicePredicateSize);
        out.writeUTF(partitioner);
        out.writeInt(port);
        out.writeUTF(host);
        split.write(out);
    }

    @Override
    public String[] getLocations() throws IOException
    {
        return split.getLocations();
    }

    @Override
    public long getLength()
    {
        return split.getLength();
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public void setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily)
    {
        this.columnFamily = columnFamily;
    }

    public int getRangeBatchSize()
    {
        return rangeBatchSize;
    }

    public void setRangeBatchSize(int rangeBatchSize)
    {
        this.rangeBatchSize = rangeBatchSize;
    }

    public int getSlicePredicateSize()
    {
        return slicePredicateSize;
    }

    public void setSlicePredicateSize(int slicePredicateSize)
    {
        this.slicePredicateSize = slicePredicateSize;
    }

    public ColumnFamilySplit getSplit()
    {
        return split;
    }

    public String getColumnMapping()
    {
        return columnMapping;
    }

    public void setColumnMapping(String mapping)
    {
        this.columnMapping = mapping;
    }

    public void setPartitioner(String part)
    {
        partitioner = part;
    }

    public String getPartitioner()
    {
        return partitioner;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    @Override
    public String toString()
    {
        return this.host + " " + this.port + " " + this.partitioner;
    }

    public void setSplitSize(int splitSize)
    {
        this.splitSize = splitSize;
    }

    public int getSplitSize()
    {
        return splitSize;
    }
}
