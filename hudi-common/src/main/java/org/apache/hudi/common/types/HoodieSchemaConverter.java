/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.types;

import org.apache.avro.Schema;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to convert between Avro Schema and HoodieSchema
 */
public class HoodieSchemaConverter {

    /**
     * Converts an Avro Schema to HoodieSchema
     * @param avroSchema the Avro schema to convert
     * @return equivalent HoodieSchema
     */
    public static HoodieSchema fromAvroSchema(Schema avroSchema) {
        if (avroSchema == null) {
            return null;
        }

        switch (avroSchema.getType()) {
            case NULL:
                return HoodieSchema.create(HoodieSchema.Type.NULL);
            case BOOLEAN:
                return HoodieSchema.create(HoodieSchema.Type.BOOLEAN);
            case INT:
                return HoodieSchema.create(HoodieSchema.Type.INT);
            case LONG:
                return HoodieSchema.create(HoodieSchema.Type.LONG);
            case FLOAT:
                return HoodieSchema.create(HoodieSchema.Type.FLOAT);
            case DOUBLE:
                return HoodieSchema.create(HoodieSchema.Type.DOUBLE);
            case BYTES:
                return HoodieSchema.create(HoodieSchema.Type.BYTES);
            case STRING:
                return HoodieSchema.create(HoodieSchema.Type.STRING);
            case RECORD:
                List<HoodieField> fields = new ArrayList<>();
                for (Schema.Field avroField : avroSchema.getFields()) {
                    HoodieField hoodieField = fromAvroField(avroField);
                    fields.add(hoodieField);
                }
                return HoodieSchema.createRecord(avroSchema.getName(), 
                    avroSchema.getNamespace(), avroSchema.getDoc(), fields);
            case ARRAY:
                HoodieSchema elementType = fromAvroSchema(avroSchema.getElementType());
                return HoodieSchema.createArray(elementType);
            case MAP:
                HoodieSchema valueType = fromAvroSchema(avroSchema.getValueType());
                return HoodieSchema.createMap(valueType);
            case UNION:
                List<HoodieSchema> unionTypes = new ArrayList<>();
                for (Schema unionSchema : avroSchema.getTypes()) {
                    unionTypes.add(fromAvroSchema(unionSchema));
                }
                return HoodieSchema.createUnion(unionTypes);
            case ENUM:
                return HoodieSchema.createEnum(avroSchema.getName(), 
                    avroSchema.getNamespace(), avroSchema.getDoc(), avroSchema.getEnumSymbols());
            case FIXED:
                return HoodieSchema.createFixed(avroSchema.getName(), 
                    avroSchema.getNamespace(), avroSchema.getDoc(), avroSchema.getFixedSize());
            default:
                throw new IllegalArgumentException("Unsupported Avro schema type: " + avroSchema.getType());
        }
    }

    /**
     * Converts an Avro Schema.Field to HoodieField
     * @param avroField the Avro field to convert
     * @return equivalent HoodieField
     */
    public static HoodieField fromAvroField(Schema.Field avroField) {
        if (avroField == null) {
            return null;
        }

        HoodieSchema fieldSchema = fromAvroSchema(avroField.schema());
        return new HoodieField(avroField.name(), fieldSchema, avroField.doc(), 
            avroField.defaultVal(), avroField.pos());
    }

    /**
     * Converts a HoodieSchema to Avro Schema
     * @param hoodieSchema the HoodieSchema to convert
     * @return equivalent Avro Schema
     */
    public static Schema toAvroSchema(HoodieSchema hoodieSchema) {
        if (hoodieSchema == null) {
            return null;
        }

        switch (hoodieSchema.getType()) {
            case NULL:
                return Schema.create(Schema.Type.NULL);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case INT:
                return Schema.create(Schema.Type.INT);
            case LONG:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case RECORD:
                List<Schema.Field> avroFields = new ArrayList<>();
                for (HoodieField hoodieField : hoodieSchema.getFields()) {
                    Schema.Field avroField = toAvroField(hoodieField);
                    avroFields.add(avroField);
                }
                Schema recordSchema = Schema.createRecord(hoodieSchema.getName(), 
                    hoodieSchema.getDoc(), hoodieSchema.getNamespace(), false);
                recordSchema.setFields(avroFields);
                return recordSchema;
            case ARRAY:
                Schema avroElementType = toAvroSchema(hoodieSchema.getElementType());
                return Schema.createArray(avroElementType);
            case MAP:
                Schema avroValueType = toAvroSchema(hoodieSchema.getValueType());
                return Schema.createMap(avroValueType);
            case UNION:
                List<Schema> avroUnionTypes = new ArrayList<>();
                for (HoodieSchema unionType : hoodieSchema.getTypes()) {
                    avroUnionTypes.add(toAvroSchema(unionType));
                }
                return Schema.createUnion(avroUnionTypes);
            case ENUM:
                return Schema.createEnum(hoodieSchema.getName(), hoodieSchema.getDoc(), 
                    hoodieSchema.getNamespace(), hoodieSchema.getEnumSymbols());
            case FIXED:
                return Schema.createFixed(hoodieSchema.getName(), hoodieSchema.getDoc(), 
                    hoodieSchema.getNamespace(), hoodieSchema.getFixedSize());
            default:
                throw new IllegalArgumentException("Unsupported HoodieSchema type: " + hoodieSchema.getType());
        }
    }

    /**
     * Converts a HoodieField to Avro Schema.Field
     * @param hoodieField the HoodieField to convert
     * @return equivalent Avro Schema.Field
     */
    public static Schema.Field toAvroField(HoodieField hoodieField) {
        if (hoodieField == null) {
            return null;
        }

        Schema avroSchema = toAvroSchema(hoodieField.schema());
        return new Schema.Field(hoodieField.name(), avroSchema, hoodieField.doc(), 
            hoodieField.defaultValue());
    }

    /**
     * Converts an Avro Schema.Type to HoodieSchema.Type
     * @param avroType the Avro type to convert
     * @return equivalent HoodieSchema.Type
     */
    public static HoodieSchema.Type fromAvroType(Schema.Type avroType) {
        if (avroType == null) {
            return null;
        }

        switch (avroType) {
            case NULL: return HoodieSchema.Type.NULL;
            case BOOLEAN: return HoodieSchema.Type.BOOLEAN;
            case INT: return HoodieSchema.Type.INT;
            case LONG: return HoodieSchema.Type.LONG;
            case FLOAT: return HoodieSchema.Type.FLOAT;
            case DOUBLE: return HoodieSchema.Type.DOUBLE;
            case BYTES: return HoodieSchema.Type.BYTES;
            case STRING: return HoodieSchema.Type.STRING;
            case RECORD: return HoodieSchema.Type.RECORD;
            case ENUM: return HoodieSchema.Type.ENUM;
            case ARRAY: return HoodieSchema.Type.ARRAY;
            case MAP: return HoodieSchema.Type.MAP;
            case UNION: return HoodieSchema.Type.UNION;
            case FIXED: return HoodieSchema.Type.FIXED;
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + avroType);
        }
    }

    /**
     * Converts a HoodieSchema.Type to Avro Schema.Type
     * @param hoodieType the HoodieSchema.Type to convert
     * @return equivalent Avro Schema.Type
     */
    public static Schema.Type toAvroType(HoodieSchema.Type hoodieType) {
        if (hoodieType == null) {
            return null;
        }

        switch (hoodieType) {
            case NULL: return Schema.Type.NULL;
            case BOOLEAN: return Schema.Type.BOOLEAN;
            case INT: return Schema.Type.INT;
            case LONG: return Schema.Type.LONG;
            case FLOAT: return Schema.Type.FLOAT;
            case DOUBLE: return Schema.Type.DOUBLE;
            case BYTES: return Schema.Type.BYTES;
            case STRING: return Schema.Type.STRING;
            case RECORD: return Schema.Type.RECORD;
            case ENUM: return Schema.Type.ENUM;
            case ARRAY: return Schema.Type.ARRAY;
            case MAP: return Schema.Type.MAP;
            case UNION: return Schema.Type.UNION;
            case FIXED: return Schema.Type.FIXED;
            default:
                throw new IllegalArgumentException("Unsupported HoodieSchema type: " + hoodieType);
        }
    }
}
