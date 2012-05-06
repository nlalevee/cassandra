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
package org.apache.cassandra.hadoop.hive.output;

import java.io.IOException;
import java.util.Properties;

import org.apache.cassandra.hadoop.hive.CassandraException;
import org.apache.cassandra.hadoop.hive.CassandraProxyClient;
import org.apache.cassandra.hadoop.hive.serde.StandardColumnSerDe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

@SuppressWarnings("deprecation")
public class HiveCassandraOutputFormat implements HiveOutputFormat<Text, CassandraPut>,
        OutputFormat<Text, CassandraPut>
{

    static final Log LOG = LogFactory.getLog(HiveCassandraOutputFormat.class);

    @Override
    public RecordWriter getHiveRecordWriter(final JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass,
            boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException
    {

        final String cassandraKeySpace = jc.get(StandardColumnSerDe.CASSANDRA_KEYSPACE_NAME);
        final String cassandraHost = jc.get(StandardColumnSerDe.CASSANDRA_HOST);
        final int cassandraPort = Integer.parseInt(jc.get(StandardColumnSerDe.CASSANDRA_PORT));

        final CassandraProxyClient client;
        try
        {
            client = new CassandraProxyClient(cassandraHost, cassandraPort, true, true);
        } catch (CassandraException e)
        {
            throw new IOException(e);
        }

        return new RecordWriter()
        {

            @Override
            public void close(boolean abort) throws IOException
            {
                if (client != null)
                {
                    client.close();
                }
            }

            @Override
            public void write(Writable w) throws IOException
            {
                if (w != null) {
                    Put put = (Put) w;
                    put.write(cassandraKeySpace, client, jc);
                }
            }

        };
    }

    @Override
    public void checkOutputSpecs(FileSystem arg0, JobConf jc) throws IOException
    {

    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<Text, CassandraPut> getRecordWriter(FileSystem arg0, JobConf arg1,
            String arg2, Progressable arg3) throws IOException
    {
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }
}
