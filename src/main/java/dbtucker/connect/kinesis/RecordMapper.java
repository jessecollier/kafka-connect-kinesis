/*
 * Copyright 2016 David Tucker
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

package dbtucker.connect.kinesis;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer;

public enum RecordMapper {
    ;

    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct()
                    .name("Kinesis.KeyValue")
                    .field("key", Schema.STRING_SCHEMA)
                    .version(1)
                    .build();

    private static final Schema KINESIS_KEY_SCHEMA =
            SchemaBuilder.map(Schema.STRING_SCHEMA, KEY_SCHEMA)
                    .name("Kinesis.Key")
                    .version(1)
                    .build();

    private static final Schema DATA_SCHEMA =
            SchemaBuilder.struct()
                    .name("Kinesis.DataValue")
                    .field("data", Schema.OPTIONAL_BYTES_SCHEMA)
                    .version(1)
                    .build();

    private static final Schema KINESIS_DATA_SCHEMA =
            SchemaBuilder.map(Schema.STRING_SCHEMA, DATA_SCHEMA)
                    .name("Kinesis.Data")
                    .version(1)
                    .build();

    public static Schema keySchema() {
        return KINESIS_KEY_SCHEMA;
    }

    public static Schema dataSchema() {
        return KINESIS_DATA_SCHEMA;
    }

    public static Map<String, Struct> packKey(String key) {
        Map<String, Struct> rval = new HashMap<>(1);

        final Struct schema = new Struct(KEY_SCHEMA);
        schema.put("key", key);

        rval.put ("schema", schema);
        return rval ;
    }

    public static Map<String, Struct> packData(ByteBuffer data) {
        Map<String, Struct> rval = new HashMap<>(1);

        final Struct value = new Struct(DATA_SCHEMA);
        value.put ("data", data);

        rval.put ("payload", value);
        return rval ;
    }

}
