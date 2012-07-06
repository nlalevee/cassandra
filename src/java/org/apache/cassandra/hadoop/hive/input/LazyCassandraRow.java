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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.hadoop.hive.serde.CassandraLazyFactory;
import org.apache.cassandra.hadoop.hive.serde.StandardColumnSerDe;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class LazyCassandraRow extends LazyStruct
{
    private List<String> cassandraColumns;
    private HiveCassandraStandardRowResult rowResult;
    private ArrayList<Object> cachedList;

    public LazyCassandraRow(LazySimpleStructObjectInspector oi)
    {
        super(oi);
    }

    public void init(HiveCassandraStandardRowResult crr, List<String> cassandraColumns,
            List<byte[]> cassandraColumnsBytes)
    {
        this.rowResult = crr;
        this.cassandraColumns = cassandraColumns;
        setParsed(false);
    }

    private void parse()
    {
        if (getFields() == null)
        {
            List<? extends StructField> fieldRefs = ((StructObjectInspector) getInspector()).getAllStructFieldRefs();
            setFields(new LazyObject[fieldRefs.size()]);
            for (int i = 0; i < getFields().length; i++)
            {
                String cassandraColumn = this.cassandraColumns.get(i);
                if (cassandraColumn.endsWith(":"))
                {
                    // want all columns as a map
                    getFields()[i] = new LazyCassandraCellMap((LazyMapObjectInspector) fieldRefs.get(i)
                            .getFieldObjectInspector());
                }
                else
                {
                    // otherwise only interested in a single column

                    getFields()[i] = CassandraLazyFactory.createLazyObject(fieldRefs.get(i).getFieldObjectInspector());
                }
            }
            setFieldInited(new boolean[getFields().length]);
        }
        Arrays.fill(getFieldInited(), false);
        setParsed(true);
    }

    @Override
    public Object getField(int fieldID)
    {
        if (!getParsed())
        {
            parse();
        }
        return uncheckedGetField(fieldID);
    }

    private Object uncheckedGetField(int fieldID)
    {
        if (!getFieldInited()[fieldID])
        {
            getFieldInited()[fieldID] = true;
            ByteArrayRef ref = null;
            String columnName = cassandraColumns.get(fieldID);

            LazyObject obj = getFields()[fieldID];
            if (columnName.equals(StandardColumnSerDe.CASSANDRA_KEY_COLUMN))
            {
                // user is asking for key column
                ref = new ByteArrayRef();
                ref.setData(rowResult.getKey().getBytes());
            }
            else if (columnName.endsWith(":"))
            {
                // user wants all columns as a map
                // TODO this into a LazyCassandraCellMap
                return null;
            }
            else
            {
                // user wants the value of a single column
                Writable res = rowResult.getValue().get(new BytesWritable(columnName.getBytes()));
                HiveIColumn hiveIColumn = (HiveIColumn) res;
                if (hiveIColumn != null)
                {
                    ref = new ByteArrayRef();
                    ref.setData(hiveIColumn.value().array());
                }
                else
                {
                    return null;
                }
            }
            if (ref != null)
            {
                obj.init(ref, 0, ref.getData().length);
            }
        }
        return getFields()[fieldID].getObject();
    }

    /**
     * Get the values of the fields as an ArrayList.
     * 
     * @return The values of the fields as an ArrayList.
     */
    @Override
    public ArrayList<Object> getFieldsAsList()
    {
        if (!getParsed())
        {
            parse();
        }
        if (cachedList == null)
        {
            cachedList = new ArrayList<Object>();
        }
        else
        {
            cachedList.clear();
        }
        for (int i = 0; i < getFields().length; i++)
        {
            cachedList.add(uncheckedGetField(i));
        }
        return cachedList;
    }

    @Override
    public Object getObject()
    {
        return this;
    }
}
