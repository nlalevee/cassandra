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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.hadoop.hive.serde.lazy.objectinspector.CassandraValidatorObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.io.Text;

public class CassandraLazyValidator extends CassandraLazyPrimitive<CassandraValidatorObjectInspector, Text>
{
    private final AbstractType validator;

    public CassandraLazyValidator(CassandraValidatorObjectInspector oi)
    {
        super(oi);
        validator = oi.getValidatorType();
        data = new Text();
    }

    public CassandraLazyValidator(CassandraLazyValidator copy)
    {
        super(copy.getInspector());
        validator = copy.validator;
        isNull = copy.isNull;
    }

    @Override
    public void parseBytes(ByteArrayRef bytes, int start, int length)
    {
        data.set(bytes.getData(), start, length);
    }

    @Override
    public void parsePrimitiveBytes(ByteArrayRef bytes, int start, int length)
    {
        ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
        setData(validator.getString(buf));
    }

    @Override
    public void setPrimitiveSize()
    {
        primitiveSize = 8;
    }

    private void setData(String str)
    {
        data.set(str);
        isNull = false;
    }

    @Override
    public boolean checkSize(int length)
    {
        return true;
    }

}
