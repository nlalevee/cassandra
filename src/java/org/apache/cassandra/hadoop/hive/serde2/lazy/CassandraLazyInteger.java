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
package org.apache.cassandra.hadoop.hive.serde2.lazy;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * CassandraLazyInteger parses the object into LongInteger value.
 * 
 */
public class CassandraLazyInteger extends CassandraLazyPrimitive<LazyIntObjectInspector, IntWritable>
{

    public CassandraLazyInteger(LazyIntObjectInspector oi)
    {
        super(oi);
        data = new IntWritable();
    }

    @Override
    public void parseBytes(ByteArrayRef bytes, int start, int length)
    {
        setData(LazyInteger.parseInt(bytes.getData(), start, length));
    }

    @Override
    public void parsePrimitiveBytes(ByteArrayRef bytes, int start, int length)
    {

        ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
        setData(buf.getInt(buf.position()));
    }

    @Override
    public void setPrimitiveSize()
    {
        primitiveSize = 4;
    }

    private void setData(int num)
    {
        data.set(num);
        isNull = false;
    }

}
