package org.apache.hudi.common.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;

/**
 * A custom implementation of Schema interface that mimics Avro's Schema without Avro dependencies.
 * This class provides the same functionality as Avro's Schema for schema representation and manipulation.
 */
public class HoodieSchema {

    /**
     * Enumeration of schema types that matches Avro's Schema.Type
     */
    public enum Type {
        RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL
    }

    private final Type type;
    private final String name;
    private final String namespace;
    private final String doc;
    private final List<HoodieField> fields;
    private final List<HoodieSchema> types; // For UNION type
    private final HoodieSchema elementType; // For ARRAY type
    private final HoodieSchema valueType;   // For MAP type
    private final int size;                 // For FIXED type
    private final List<String> symbols;     // For ENUM type
    private final Map<String, Object> props; // Additional properties

    // Private constructor for builder pattern
    private HoodieSchema(Builder builder) {
        this.type = builder.type;
        this.name = builder.name;
        this.namespace = builder.namespace;
        this.doc = builder.doc;
        this.fields = builder.fields != null ? Collections.unmodifiableList(new ArrayList<>(builder.fields)) : null;
        this.types = builder.types != null ? Collections.unmodifiableList(new ArrayList<>(builder.types)) : null;
        this.elementType = builder.elementType;
        this.valueType = builder.valueType;
        this.size = builder.size;
        this.symbols = builder.symbols != null ? Collections.unmodifiableList(new ArrayList<>(builder.symbols)) : null;
        this.props = builder.props != null ? Collections.unmodifiableMap(new HashMap<>(builder.props)) : Collections.emptyMap();
    }

    /**
     * Gets the type of this schema
     * @return the schema type
     */
    public Type getType() {
        return type;
    }

    /**
     * Gets the name of this schema (for named types like RECORD, ENUM, FIXED)
     * @return the schema name, or null for unnamed types
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the namespace of this schema
     * @return the namespace, or null if not set
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Gets the full name (namespace + name) of this schema
     * @return the full name, or just name if no namespace
     */
    public String getFullName() {
        if (namespace == null || namespace.isEmpty()) {
            return name;
        }
        return namespace + "." + name;
    }

    /**
     * Gets the documentation string for this schema
     * @return the doc string, or null if not set
     */
    public String getDoc() {
        return doc;
    }

    /**
     * Gets the fields of this schema (for RECORD type only)
     * @return list of fields for RECORD schemas
     * @throws IllegalStateException if this is not a RECORD schema
     */
    public List<HoodieField> getFields() {
        if (type != Type.RECORD) {
            throw new IllegalStateException("getFields() is only supported for RECORD type schemas, got: " + type);
        }
        return fields != null ? fields : Collections.emptyList();
    }

    /**
     * Gets a field by name (for RECORD type only)
     * @param name the field name
     * @return the field, or null if not found
     * @throws IllegalStateException if this is not a RECORD schema
     */
    public HoodieField getField(String name) {
        if (type != Type.RECORD) {
            throw new IllegalStateException("getField() is only supported for RECORD type schemas, got: " + type);
        }
        if (fields == null) {
            return null;
        }
        return fields.stream()
                .filter(field -> Objects.equals(field.name(), name))
                .findFirst()
                .orElse(null);
    }

    /**
     * Gets the union types (for UNION type only)
     * @return list of schemas in the union
     * @throws IllegalStateException if this is not a UNION schema
     */
    public List<HoodieSchema> getTypes() {
        if (type != Type.UNION) {
            throw new IllegalStateException("getTypes() is only supported for UNION type schemas, got: " + type);
        }
        return types != null ? types : Collections.emptyList();
    }

    /**
     * Gets the element type (for ARRAY type only)
     * @return the schema of array elements
     * @throws IllegalStateException if this is not an ARRAY schema
     */
    public HoodieSchema getElementType() {
        if (type != Type.ARRAY) {
            throw new IllegalStateException("getElementType() is only supported for ARRAY type schemas, got: " + type);
        }
        return elementType;
    }

    /**
     * Gets the value type (for MAP type only)
     * @return the schema of map values
     * @throws IllegalStateException if this is not a MAP schema
     */
    public HoodieSchema getValueType() {
        if (type != Type.MAP) {
            throw new IllegalStateException("getValueType() is only supported for MAP type schemas, got: " + type);
        }
        return valueType;
    }

    /**
     * Gets the fixed size (for FIXED type only)
     * @return the size of the fixed type
     * @throws IllegalStateException if this is not a FIXED schema
     */
    public int getFixedSize() {
        if (type != Type.FIXED) {
            throw new IllegalStateException("getFixedSize() is only supported for FIXED type schemas, got: " + type);
        }
        return size;
    }

    /**
     * Gets the enum symbols (for ENUM type only)
     * @return list of enum symbols
     * @throws IllegalStateException if this is not an ENUM schema
     */
    public List<String> getEnumSymbols() {
        if (type != Type.ENUM) {
            throw new IllegalStateException("getEnumSymbols() is only supported for ENUM type schemas, got: " + type);
        }
        return symbols != null ? symbols : Collections.emptyList();
    }

    /**
     * Gets a property value
     * @param key the property key
     * @return the property value, or null if not found
     */
    public Object getProp(String key) {
        return props.get(key);
    }

    /**
     * Gets all properties
     * @return map of all properties
     */
    public Map<String, Object> getProps() {
        return props;
    }

    @Override
    public String toString() {
        // Simple JSON-like representation
        switch (type) {
            case NULL:
                return "\"null\"";
            case BOOLEAN:
                return "\"boolean\"";
            case INT:
                return "\"int\"";
            case LONG:
                return "\"long\"";
            case FLOAT:
                return "\"float\"";
            case DOUBLE:
                return "\"double\"";
            case BYTES:
                return "\"bytes\"";
            case STRING:
                return "\"string\"";
            case RECORD:
                StringBuilder sb = new StringBuilder();
                sb.append("{\"type\":\"record\"");
                if (name != null) {
                    sb.append(",\"name\":\"").append(name).append("\"");
                }
                if (namespace != null) {
                    sb.append(",\"namespace\":\"").append(namespace).append("\"");
                }
                if (fields != null && !fields.isEmpty()) {
                    sb.append(",\"fields\":[");
                    for (int i = 0; i < fields.size(); i++) {
                        if (i > 0) sb.append(",");
                        sb.append(fields.get(i).toString());
                    }
                    sb.append("]");
                }
                sb.append("}");
                return sb.toString();
            case ARRAY:
                return "{\"type\":\"array\",\"items\":" + (elementType != null ? elementType.toString() : "null") + "}";
            case MAP:
                return "{\"type\":\"map\",\"values\":" + (valueType != null ? valueType.toString() : "null") + "}";
            case UNION:
                StringBuilder unionSb = new StringBuilder();
                unionSb.append("[");
                if (types != null) {
                    for (int i = 0; i < types.size(); i++) {
                        if (i > 0) unionSb.append(",");
                        unionSb.append(types.get(i).toString());
                    }
                }
                unionSb.append("]");
                return unionSb.toString();
            case ENUM:
                StringBuilder enumSb = new StringBuilder();
                enumSb.append("{\"type\":\"enum\"");
                if (name != null) {
                    enumSb.append(",\"name\":\"").append(name).append("\"");
                }
                if (symbols != null) {
                    enumSb.append(",\"symbols\":[");
                    for (int i = 0; i < symbols.size(); i++) {
                        if (i > 0) enumSb.append(",");
                        enumSb.append("\"").append(symbols.get(i)).append("\"");
                    }
                    enumSb.append("]");
                }
                enumSb.append("}");
                return enumSb.toString();
            case FIXED:
                StringBuilder fixedSb = new StringBuilder();
                fixedSb.append("{\"type\":\"fixed\"");
                if (name != null) {
                    fixedSb.append(",\"name\":\"").append(name).append("\"");
                }
                fixedSb.append(",\"size\":").append(size);
                fixedSb.append("}");
                return fixedSb.toString();
            default:
                return "\"" + type.name().toLowerCase() + "\"";
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        HoodieSchema that = (HoodieSchema) obj;
        return size == that.size &&
                type == that.type &&
                Objects.equals(name, that.name) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(doc, that.doc) &&
                Objects.equals(fields, that.fields) &&
                Objects.equals(types, that.types) &&
                Objects.equals(elementType, that.elementType) &&
                Objects.equals(valueType, that.valueType) &&
                Objects.equals(symbols, that.symbols) &&
                Objects.equals(props, that.props);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, namespace, doc, fields, types, elementType, valueType, size, symbols, props);
    }

    // Static factory methods

    /**
     * Creates a primitive schema
     * @param type the primitive type
     * @return a new schema of the specified type
     */
    public static HoodieSchema create(Type type) {
        switch (type) {
            case NULL:
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
            case STRING:
                return new Builder(type).build();
            default:
                throw new IllegalArgumentException("create(Type) only supports primitive types, got: " + type);
        }
    }

    /**
     * Creates a record schema
     * @param name the record name
     * @param namespace the namespace (can be null)
     * @param doc the documentation (can be null)
     * @param fields the list of fields
     * @return a new record schema
     */
    public static HoodieSchema createRecord(String name, String namespace, String doc, List<HoodieField> fields) {
        return new Builder(Type.RECORD)
                .setName(name)
                .setNamespace(namespace)
                .setDoc(doc)
                .setFields(fields)
                .build();
    }

    /**
     * Creates an array schema
     * @param elementType the type of array elements
     * @return a new array schema
     */
    public static HoodieSchema createArray(HoodieSchema elementType) {
        return new Builder(Type.ARRAY)
                .setElementType(elementType)
                .build();
    }

    /**
     * Creates a map schema
     * @param valueType the type of map values
     * @return a new map schema
     */
    public static HoodieSchema createMap(HoodieSchema valueType) {
        return new Builder(Type.MAP)
                .setValueType(valueType)
                .build();
    }

    /**
     * Creates a union schema
     * @param types the list of types in the union
     * @return a new union schema
     */
    public static HoodieSchema createUnion(List<HoodieSchema> types) {
        return new Builder(Type.UNION)
                .setTypes(types)
                .build();
    }

    /**
     * Creates an enum schema
     * @param name the enum name
     * @param namespace the namespace (can be null)
     * @param doc the documentation (can be null)
     * @param symbols the list of enum symbols
     * @return a new enum schema
     */
    public static HoodieSchema createEnum(String name, String namespace, String doc, List<String> symbols) {
        return new Builder(Type.ENUM)
                .setName(name)
                .setNamespace(namespace)
                .setDoc(doc)
                .setSymbols(symbols)
                .build();
    }

    /**
     * Creates a fixed schema
     * @param name the fixed name
     * @param namespace the namespace (can be null)
     * @param doc the documentation (can be null)
     * @param size the size of the fixed type
     * @return a new fixed schema
     */
    public static HoodieSchema createFixed(String name, String namespace, String doc, int size) {
        return new Builder(Type.FIXED)
                .setName(name)
                .setNamespace(namespace)
                .setDoc(doc)
                .setSize(size)
                .build();
    }

    /**
     * Parse a schema from a JSON string representation
     * This is a simplified implementation that supports basic parsing
     * @param jsonString the JSON schema string
     * @return a new HoodieSchema instance
     * @throws IllegalArgumentException if the JSON string is invalid or unsupported
     */
    public static HoodieSchema parse(String jsonString) {
        // This is a simplified implementation. In a real scenario, you would use a JSON parser
        // For now, we'll support basic type parsing
        if (jsonString == null || jsonString.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema string cannot be null or empty");
        }
        
        String trimmed = jsonString.trim();
        
        // Handle primitive types
        if ("\"null\"".equals(trimmed)) return create(Type.NULL);
        if ("\"boolean\"".equals(trimmed)) return create(Type.BOOLEAN);
        if ("\"int\"".equals(trimmed)) return create(Type.INT);
        if ("\"long\"".equals(trimmed)) return create(Type.LONG);
        if ("\"float\"".equals(trimmed)) return create(Type.FLOAT);
        if ("\"double\"".equals(trimmed)) return create(Type.DOUBLE);
        if ("\"bytes\"".equals(trimmed)) return create(Type.BYTES);
        if ("\"string\"".equals(trimmed)) return create(Type.STRING);
        
        // For complex types, we'd need a full JSON parser
        // This is a placeholder for basic functionality
        throw new IllegalArgumentException("Complex schema parsing not yet implemented: " + jsonString);
    }

    /**
     * Creates a nullable union schema (union with null)
     * @param schema the schema to make nullable
     * @return a union schema containing null and the provided schema
     */
    public static HoodieSchema createNullableUnion(HoodieSchema schema) {
        List<HoodieSchema> types = new ArrayList<>();
        types.add(create(Type.NULL));
        types.add(schema);
        return createUnion(types);
    }

    /**
     * Creates a union schema from multiple schemas
     * @param schemas the schemas to include in the union
     * @return a union schema
     */
    public static HoodieSchema createUnion(HoodieSchema... schemas) {
        List<HoodieSchema> typeList = new ArrayList<>();
        for (HoodieSchema schema : schemas) {
            typeList.add(schema);
        }
        return createUnion(typeList);
    }

    /**
     * Builder class for constructing HoodieSchema instances
     */
    public static class Builder {
        private final Type type;
        private String name;
        private String namespace;
        private String doc;
        private List<HoodieField> fields;
        private List<HoodieSchema> types;
        private HoodieSchema elementType;
        private HoodieSchema valueType;
        private int size;
        private List<String> symbols;
        private Map<String, Object> props;

        public Builder(Type type) {
            this.type = Objects.requireNonNull(type, "Schema type cannot be null");
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder setDoc(String doc) {
            this.doc = doc;
            return this;
        }

        public Builder setFields(List<HoodieField> fields) {
            this.fields = fields;
            return this;
        }

        public Builder setTypes(List<HoodieSchema> types) {
            this.types = types;
            return this;
        }

        public Builder setElementType(HoodieSchema elementType) {
            this.elementType = elementType;
            return this;
        }

        public Builder setValueType(HoodieSchema valueType) {
            this.valueType = valueType;
            return this;
        }

        public Builder setSize(int size) {
            this.size = size;
            return this;
        }

        public Builder setSymbols(List<String> symbols) {
            this.symbols = symbols;
            return this;
        }

        public Builder setProp(String key, Object value) {
            if (props == null) {
                props = new HashMap<>();
            }
            props.put(key, value);
            return this;
        }

        public Builder setProps(Map<String, Object> props) {
            this.props = props;
            return this;
        }

        public HoodieSchema build() {
            return new HoodieSchema(this);
        }
    }
}
