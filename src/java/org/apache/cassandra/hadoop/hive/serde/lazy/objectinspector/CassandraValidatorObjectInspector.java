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
package org.apache.cassandra.hadoop.hive.serde.lazy.objectinspector;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.hadoop.hive.serde2.lazy.CassandraLazyValidator;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.AbstractPrimitiveLazyObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class CassandraValidatorObjectInspector extends AbstractPrimitiveLazyObjectInspector<Text> implements
        StringObjectInspector
{

    /**
     * The primitive types supported by Hive.
     */
    public static enum CassandraValidatorCategory
    {
        UUID, ASCII, BYTES, INTEGER, LONG, UTF8, UNKNOWN
    }

    private final AbstractType validator;

    CassandraValidatorObjectInspector(AbstractType type)
    {
        super(PrimitiveObjectInspectorUtils.stringTypeEntry);
        validator = type;
    }

    @Override
    public String getTypeName()
    {
        return PrimitiveObjectInspectorUtils.stringTypeEntry.typeName;
    }

    @Override
    public Category getCategory()
    {
        return Category.PRIMITIVE;
    }

    public AbstractType getValidatorType()
    {
        return validator;
    }

    @Override
    public String getPrimitiveJavaObject(Object o)
    {
        return o == null ? null : ((CassandraLazyValidator) o).getWritableObject().toString();
    }

    @Override
    public Object copyObject(Object o)
    {
        return o == null ? null : new CassandraLazyValidator((CassandraLazyValidator) o);
    }
}
