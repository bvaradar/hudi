package org.apache.hudi.common.types;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A custom implementation of Field interface that mimics Avro's Schema.Field without Avro dependencies.
 * This class represents a field within a record schema, providing the same functionality as Avro's Field.
 */
public class HoodieField {

    private final String name;
    private final HoodieSchema schema;
    private final String doc;
    private final Object defaultValue;
    private final int pos;
    private final Map<String, Object> props;

    // Private constructor for builder pattern
    private HoodieField(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "Field name cannot be null");
        this.schema = Objects.requireNonNull(builder.schema, "Field schema cannot be null");
        this.doc = builder.doc;
        this.defaultValue = builder.defaultValue;
        this.pos = builder.pos;
        this.props = builder.props != null ? Collections.unmodifiableMap(new HashMap<>(builder.props)) : Collections.emptyMap();
    }

    /**
     * Constructor for creating a field with name and schema
     * @param name the field name
     * @param schema the field schema
     */
    public HoodieField(String name, HoodieSchema schema) {
        this(name, schema, null, null, -1);
    }

    /**
     * Constructor for creating a field with name, schema, and doc
     * @param name the field name
     * @param schema the field schema
     * @param doc the documentation string
     */
    public HoodieField(String name, HoodieSchema schema, String doc) {
        this(name, schema, doc, null, -1);
    }

    /**
     * Constructor for creating a field with name, schema, doc, and default value
     * @param name the field name
     * @param schema the field schema
     * @param doc the documentation string
     * @param defaultValue the default value
     */
    public HoodieField(String name, HoodieSchema schema, String doc, Object defaultValue) {
        this(name, schema, doc, defaultValue, -1);
    }

    /**
     * Constructor for creating a field with all properties
     * @param name the field name
     * @param schema the field schema
     * @param doc the documentation string
     * @param defaultValue the default value
     * @param pos the position in the record
     */
    public HoodieField(String name, HoodieSchema schema, String doc, Object defaultValue, int pos) {
        this.name = Objects.requireNonNull(name, "Field name cannot be null");
        this.schema = Objects.requireNonNull(schema, "Field schema cannot be null");
        this.doc = doc;
        this.defaultValue = defaultValue;
        this.pos = pos;
        this.props = Collections.emptyMap();
    }

    /**
     * Gets the name of this field
     * @return the field name
     */
    public String name() {
        return name;
    }

    /**
     * Gets the schema of this field
     * @return the field schema
     */
    public HoodieSchema schema() {
        return schema;
    }

    /**
     * Gets the documentation string for this field
     * @return the doc string, or null if not set
     */
    public String doc() {
        return doc;
    }

    /**
     * Gets the default value for this field
     * @return the default value, or null if not set
     */
    public Object defaultValue() {
        return defaultValue;
    }

    /**
     * Checks if this field has a default value
     * @return true if this field has a default value
     */
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    /**
     * Gets the position of this field in the record
     * @return the field position, or -1 if not set
     */
    public int pos() {
        return pos;
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
        StringBuilder sb = new StringBuilder();
        sb.append("{\"name\":\"").append(name).append("\"");
        sb.append(",\"type\":").append(schema.toString());
        
        if (doc != null) {
            sb.append(",\"doc\":\"").append(doc).append("\"");
        }
        
        if (defaultValue != null) {
            sb.append(",\"default\":");
            if (defaultValue instanceof String) {
                sb.append("\"").append(defaultValue).append("\"");
            } else {
                sb.append(defaultValue.toString());
            }
        }
        
        if (!props.isEmpty()) {
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                sb.append(",\"").append(entry.getKey()).append("\":");
                Object value = entry.getValue();
                if (value instanceof String) {
                    sb.append("\"").append(value).append("\"");
                } else {
                    sb.append(value.toString());
                }
            }
        }
        
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        HoodieField that = (HoodieField) obj;
        return pos == that.pos &&
                Objects.equals(name, that.name) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(doc, that.doc) &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(props, that.props);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schema, doc, defaultValue, pos, props);
    }

    /**
     * Creates a new field with the specified name and schema
     * @param name the field name
     * @param schema the field schema
     * @return a new HoodieField instance
     */
    public static HoodieField of(String name, HoodieSchema schema) {
        return new HoodieField(name, schema);
    }

    /**
     * Creates a new field with the specified name, schema, and doc
     * @param name the field name
     * @param schema the field schema
     * @param doc the documentation string
     * @return a new HoodieField instance
     */
    public static HoodieField of(String name, HoodieSchema schema, String doc) {
        return new HoodieField(name, schema, doc);
    }

    /**
     * Creates a new field with the specified name, schema, doc, and default value
     * @param name the field name
     * @param schema the field schema
     * @param doc the documentation string
     * @param defaultValue the default value
     * @return a new HoodieField instance
     */
    public static HoodieField of(String name, HoodieSchema schema, String doc, Object defaultValue) {
        return new HoodieField(name, schema, doc, defaultValue);
    }

    /**
     * Builder class for constructing HoodieField instances
     */
    public static class Builder {
        private String name;
        private HoodieSchema schema;
        private String doc;
        private Object defaultValue;
        private int pos = -1;
        private Map<String, Object> props;

        public Builder(String name, HoodieSchema schema) {
            this.name = Objects.requireNonNull(name, "Field name cannot be null");
            this.schema = Objects.requireNonNull(schema, "Field schema cannot be null");
        }

        public Builder setDoc(String doc) {
            this.doc = doc;
            return this;
        }

        public Builder setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder setPos(int pos) {
            this.pos = pos;
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

        public HoodieField build() {
            return new HoodieField(this);
        }
    }
}
