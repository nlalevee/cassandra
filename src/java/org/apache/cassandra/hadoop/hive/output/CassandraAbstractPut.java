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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.hive.CassandraProxyClient;
import org.apache.cassandra.hadoop.hive.serde.StandardColumnSerDe;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

public abstract class CassandraAbstractPut implements Put
{

    /**
     * Parse batch mutation size from job configuration. If none is defined, return the default value 500.
     * 
     * @param jc
     *            job configuration
     * @return batch mutation size
     */
    protected int getBatchMutationSize(JobConf jc)
    {
        return jc.getInt(StandardColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
                StandardColumnSerDe.DEFAULT_BATCH_MUTATION_SIZE);
    }

    /**
     * Parse pause length in ms between two batch mutations from job configuration. If none is defined, return the default value 100 ms.
     * 
     * @param jc job configuration
     * @return batch mutation pause length
     */
    protected long getBatchMutationPause(JobConf jc)
    {
        return jc.getLong(StandardColumnSerDe.CASSANDRA_BATCH_MUTATION_PAUSE,
                StandardColumnSerDe.DEFAULT_BATCH_MUTATION_PAUSE);
    }

    /**
     * Parse consistency level from job configuration. If none is defined, or if the specified value is not a valid
     * <code>ConsistencyLevel</code>, return default consistency level ONE.
     * 
     * @param jc
     *            job configuration
     * @return cassandra consistency level
     */
    protected static ConsistencyLevel getConsistencyLevel(JobConf jc)
    {
        String consistencyLevel = jc.get(StandardColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,
                StandardColumnSerDe.DEFAULT_CONSISTENCY_LEVEL);
        ConsistencyLevel level = null;
        try
        {
            level = ConsistencyLevel.valueOf(consistencyLevel);
        } catch (IllegalArgumentException e)
        {
            level = ConsistencyLevel.QUORUM;
        }

        return level;
    }

    /**
     * Commit the changes in mutation map to cassandra client for given keyspace with given consistency level.
     * 
     * @param keySpace
     *            cassandra key space
     * @param client
     *            cassandra client
     * @param flevel
     *            cassandra consistency level
     * @param mutation_map
     *            cassandra mutation map
     * @throws IOException
     *             when error happens in batch mutate
     */
    protected void commitChanges(String keySpace, CassandraProxyClient client, ConsistencyLevel flevel,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map) throws IOException
    {
        try
        {
            client.getProxyConnection().set_keyspace(keySpace);
            client.getProxyConnection().batch_mutate(mutation_map, flevel);
        } catch (InvalidRequestException e)
        {
            throw new IOException(e);
        } catch (UnavailableException e)
        {
            throw new IOException(e);
        } catch (TimedOutException e)
        {
            throw new IOException(e);
        } catch (TException e)
        {
            throw new IOException(e);
        }
    }
}
