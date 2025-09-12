package org.apache.hudi.common.types;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Hudi Schema class that provides binary compatibility with Apache Avro Schema.
 */
public class Schema implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Logical type interface for schema types that represent logical interpretations of primitive types.
   */
  public interface LogicalType extends Serializable {
    String getName();

    default void validate(Schema schema) {
      // Default implementation does nothing
    }
  }

  /**
   * Built-in logical types
   */
  public static class LogicalTypes {

    public static class Decimal implements LogicalType {
      private final int precision;
      private final int scale;

      public Decimal(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
      }

      @Override
      public String getName() {
        return "decimal";
      }

      public int getPrecision() {
        return precision;
      }

      public int getScale() {
        return scale;
      }

      @Override
      public void validate(Schema schema) {
        if (schema.getType() != Type.BYTES && schema.getType() != Type.FIXED) {
          throw new IllegalArgumentException("Decimal can only be used with bytes or fixed types");
        }
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Decimal)) return false;
        Decimal decimal = (Decimal) obj;
        return precision == decimal.precision && scale == decimal.scale;
      }

      @Override
      public int hashCode() {
        return Objects.hash(precision, scale);
      }
    }

    public static class TimestampMillis implements LogicalType {
      @Override
      public String getName() {
        return "timestamp-millis";
      }

      @Override
      public void validate(Schema schema) {
        if (schema.getType() != Type.LONG) {
          throw new IllegalArgumentException("timestamp-millis can only be used with long type");
        }
      }
    }

    public static class TimestampMicros implements LogicalType {
      @Override
      public String getName() {
        return "timestamp-micros";
      }

      @Override
      public void validate(Schema schema) {
        if (schema.getType() != Type.LONG) {
          throw new IllegalArgumentException("timestamp-micros can only be used with long type");
        }
      }
    }

    public static class TimeMillis implements LogicalType {
      @Override
      public String getName() {
        return "time-millis";
      }

      @Override
      public void validate(Schema schema) {
        if (schema.getType() != Type.INT) {
          throw new IllegalArgumentException("time-millis can only be used with int type");
        }
      }
    }

    public static class TimeMicros implements LogicalType {
      @Override
      public String getName() {
        return "time-micros";
      }

      @Override
      public void validate(Schema schema) {
        if (schema.getType() != Type.LONG) {
          throw new IllegalArgumentException("time-micros can only be used with long type");
        }
      }
    }

    public static class Date implements LogicalType {
      @Override
      public String getName() {
        return "date";
      }

      @Override
      public void validate(Schema schema) {
        if (schema.getType() != Type.INT) {
          throw new IllegalArgumentException("date can only be used with int type");
        }
      }
    }

    public static class Uuid implements LogicalType {
      @Override
      public String getName() {
        return "uuid";
      }

      @Override
      public void validate(Schema schema) {
        if (schema.getType() != Type.STRING) {
          throw new IllegalArgumentException("uuid can only be used with string type");
        }
      }
    }

    public static LogicalType decimal(int precision, int scale) {
      return new Decimal(precision, scale);
    }

    public static LogicalType timestampMillis() {
      return new TimestampMillis();
    }

    public static LogicalType timestampMicros() {
      return new TimestampMicros();
    }

    public static LogicalType timeMillis() {
      return new TimeMillis();
    }

    public static LogicalType timeMicros() {
      return new TimeMicros();
    }

    public static LogicalType date() {
      return new Date();
    }

    public static LogicalType uuid() {
      return new Uuid();
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public enum Type {
    RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;

    public String getName() {
      return name().toLowerCase();
    }
  }

  public static class Field implements Serializable {
    private final String name;
    private final Schema schema;
    private final String doc;
    private final JsonNode defaultVal;
    private final Order order;
    private final Set<String> aliases;
    private final Map<String, Object> props;
    private int pos = -1; // Field position in record

    public enum Order {
      ASCENDING, DESCENDING, IGNORE
    }

    public Field(String name, Schema schema) {
      this(name, schema, null, null);
    }

    public Field(String name, Schema schema, String doc) {
      this(name, schema, doc, null);
    }

    public Field(String name, Schema schema, String doc, JsonNode defaultVal) {
      this(name, schema, doc, defaultVal, Order.ASCENDING);
    }

    public Field(String name, Schema schema, String doc, JsonNode defaultVal, Order order) {
      this.name = name;
      this.schema = schema;
      this.doc = doc;
      this.defaultVal = defaultVal;
      this.order = order;
      this.aliases = new HashSet<>();
      this.props = new HashMap<>();
    }

    public String name() {
      return name;
    }

    public Schema schema() {
      return schema;
    }

    public String doc() {
      return doc;
    }

    public JsonNode defaultVal() {
      return defaultVal;
    }

    public Order order() {
      return order;
    }

    public Set<String> aliases() {
      return aliases;
    }

    public boolean hasDefaultValue() {
      return defaultVal != null;
    }

    public void addAlias(String alias) {
      aliases.add(alias);
    }

    public int pos() {
      return pos;
    }

    public void setPos(int pos) {
      this.pos = pos;
    }

    public void addProp(String name, Object value) {
      props.put(name, value);
    }

    public Object getProp(String name) {
      return props.get(name);
    }

    public Map<String, Object> getObjectProps() {
      return new HashMap<>(props);
    }
  }

  private final Type type;
  private final String name;
  private final String namespace;
  private final String doc;
  private final List<Field> fields;
  private final List<String> symbols;
  private final Schema elementType;
  private final Schema valueType;
  private final List<Schema> types;
  private final int fixedSize;
  private final String fullName;
  private final Set<String> aliases;
  private final Map<String, Object> props;
  private LogicalType logicalType;

  // Private constructor for builder
  private Schema(Type type, String name, String namespace, String doc,
                 List<Field> fields, List<String> symbols, Schema elementType,
                 Schema valueType, List<Schema> types, int fixedSize) {
    this.type = type;
    this.name = name;
    this.namespace = namespace;
    this.doc = doc;
    this.fields = fields != null ? new ArrayList<>(fields) : null;
    this.symbols = symbols != null ? new ArrayList<>(symbols) : null;
    this.elementType = elementType;
    this.valueType = valueType;
    this.types = types != null ? new ArrayList<>(types) : null;
    this.fixedSize = fixedSize;
    this.fullName = computeFullName(name, namespace);
    this.aliases = new HashSet<>();
    this.props = new HashMap<>();
    this.logicalType = null;
    
    // Set field positions for record schemas
    if (this.fields != null) {
      for (int i = 0; i < this.fields.size(); i++) {
        this.fields.get(i).setPos(i);
      }
    }
  }

  // Static factory methods for primitive types
  public static Schema create(Type type) {
    return new Schema(type, null, null, null, null, null, null, null, null, 0);
  }

  public static Schema createRecord(String name, String doc, String namespace, boolean isError) {
    return new Schema(Type.RECORD, name, namespace, doc, new ArrayList<>(), null, null, null, null, 0);
  }

  public static Schema createEnum(String name, String doc, String namespace, List<String> symbols) {
    return new Schema(Type.ENUM, name, namespace, doc, null, symbols, null, null, null, 0);
  }

  public static Schema createArray(Schema elementType) {
    return new Schema(Type.ARRAY, null, null, null, null, null, elementType, null, null, 0);
  }

  public static Schema createMap(Schema valueType) {
    return new Schema(Type.MAP, null, null, null, null, null, null, valueType, null, 0);
  }

  public static Schema createUnion(List<Schema> types) {
    return new Schema(Type.UNION, null, null, null, null, null, null, null, types, 0);
  }

  public static Schema createFixed(String name, String doc, String namespace, int size) {
    return new Schema(Type.FIXED, name, namespace, doc, null, null, null, null, null, size);
  }

  /**
   * Schema parser for compatibility with Avro Schema.Parser
   */
  public static class Parser {
    public Schema parse(String jsonSchema) {
      return Schema.parse(jsonSchema);
    }
  }

  // Parse from JSON string
  public static Schema parse(String jsonSchema) {
    try {
      JsonNode node = MAPPER.readTree(jsonSchema);
      return parseSchema(node);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse schema", e);
    }
  }

  private static Schema parseSchema(JsonNode node) {
    if (node.isTextual()) {
      String typeName = node.asText();
      Type type = Type.valueOf(typeName.toUpperCase());
      return create(type);
    }

    if (node.isArray()) {
      List<Schema> unionTypes = new ArrayList<>();
      for (JsonNode unionNode : node) {
        unionTypes.add(parseSchema(unionNode));
      }
      return createUnion(unionTypes);
    }

    ObjectNode objNode = (ObjectNode) node;
    String typeName = objNode.get("type").asText();
    Type type = Type.valueOf(typeName.toUpperCase());

    switch (type) {
      case RECORD:
        String name = objNode.get("name").asText();
        String namespace = objNode.has("namespace") ? objNode.get("namespace").asText() : null;
        String doc = objNode.has("doc") ? objNode.get("doc").asText() : null;

        Schema recordSchema = createRecord(name, doc, namespace, false);

        if (objNode.has("fields")) {
          ArrayNode fieldsNode = (ArrayNode) objNode.get("fields");
          int pos = 0;
          for (JsonNode fieldNode : fieldsNode) {
            Field field = parseField(fieldNode);
            field.setPos(pos++);
            recordSchema.fields.add(field);
          }
        }
        
        // Parse logical type if present
        if (objNode.has("logicalType")) {
          String logicalTypeName = objNode.get("logicalType").asText();
          recordSchema.setLogicalType(parseLogicalType(logicalTypeName, objNode));
        }
        
        return recordSchema;

      case ENUM:
        name = objNode.get("name").asText();
        namespace = objNode.has("namespace") ? objNode.get("namespace").asText() : null;
        doc = objNode.has("doc") ? objNode.get("doc").asText() : null;
        List<String> symbols = new ArrayList<>();
        ArrayNode symbolsNode = (ArrayNode) objNode.get("symbols");
        for (JsonNode symbolNode : symbolsNode) {
          symbols.add(symbolNode.asText());
        }
        return createEnum(name, doc, namespace, symbols);

      case ARRAY:
        Schema itemSchema = parseSchema(objNode.get("items"));
        return createArray(itemSchema);

      case MAP:
        Schema valuesSchema = parseSchema(objNode.get("values"));
        return createMap(valuesSchema);

      case FIXED:
        name = objNode.get("name").asText();
        namespace = objNode.has("namespace") ? objNode.get("namespace").asText() : null;
        doc = objNode.has("doc") ? objNode.get("doc").asText() : null;
        int size = objNode.get("size").asInt();
        return createFixed(name, doc, namespace, size);

      default:
        Schema primitiveSchema = create(type);
        // Parse logical type for primitive types
        if (objNode.has("logicalType")) {
          String logicalTypeName = objNode.get("logicalType").asText();
          primitiveSchema.setLogicalType(parseLogicalType(logicalTypeName, objNode));
        }
        return primitiveSchema;
    }
  }

  private static LogicalType parseLogicalType(String typeName, ObjectNode objNode) {
    switch (typeName) {
      case "decimal":
        int precision = objNode.get("precision").asInt();
        int scale = objNode.has("scale") ? objNode.get("scale").asInt() : 0;
        return LogicalTypes.decimal(precision, scale);
      case "timestamp-millis":
        return LogicalTypes.timestampMillis();
      case "timestamp-micros":
        return LogicalTypes.timestampMicros();
      case "time-millis":
        return LogicalTypes.timeMillis();
      case "time-micros":
        return LogicalTypes.timeMicros();
      case "date":
        return LogicalTypes.date();
      case "uuid":
        return LogicalTypes.uuid();
      default:
        // For unknown logical types, return null and let the schema be treated as its underlying type
        return null;
    }
  }

  private static Field parseField(JsonNode fieldNode) {
    String fieldName = fieldNode.get("name").asText();
    Schema fieldSchema = parseSchema(fieldNode.get("type"));
    String fieldDoc = fieldNode.has("doc") ? fieldNode.get("doc").asText() : null;
    JsonNode defaultVal = fieldNode.has("default") ? fieldNode.get("default") : null;

    return new Field(fieldName, fieldSchema, fieldDoc, defaultVal);
  }

  // Convert to Avro Schema
  public org.apache.avro.Schema toAvroSchema() {
    return org.apache.avro.Schema.parse(toString());
  }

  // Convert from Avro Schema
  public static Schema fromAvroSchema(org.apache.avro.Schema avroSchema) {
    return parse(avroSchema.toString());
  }

  @Override
  public String toString() {
    try {
      return MAPPER.writeValueAsString(toJson());
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize schema", e);
    }
  }

  private JsonNode toJson() {
    ObjectNode node = MAPPER.createObjectNode();

    switch (type) {
      case RECORD:
        node.put("type", "record");
        node.put("name", name);
        if (namespace != null) node.put("namespace", namespace);
        if (doc != null) node.put("doc", doc);

        ArrayNode fieldsArray = MAPPER.createArrayNode();
        if (fields != null) {
          for (Field field : fields) {
            ObjectNode fieldNode = MAPPER.createObjectNode();
            fieldNode.put("name", field.name());
            fieldNode.set("type", field.schema().toJson());
            if (field.doc() != null) fieldNode.put("doc", field.doc());
            if (field.defaultVal() != null) fieldNode.set("default", field.defaultVal());
            fieldsArray.add(fieldNode);
          }
        }
        node.set("fields", fieldsArray);
        break;

      case ENUM:
        node.put("type", "enum");
        node.put("name", name);
        if (namespace != null) node.put("namespace", namespace);
        if (doc != null) node.put("doc", doc);

        ArrayNode symbolsArray = MAPPER.createArrayNode();
        if (symbols != null) {
          for (String symbol : symbols) {
            symbolsArray.add(symbol);
          }
        }
        node.set("symbols", symbolsArray);
        break;

      case ARRAY:
        node.put("type", "array");
        node.set("items", elementType.toJson());
        break;

      case MAP:
        node.put("type", "map");
        node.set("values", valueType.toJson());
        break;

      case UNION:
        ArrayNode unionArray = MAPPER.createArrayNode();
        if (types != null) {
          for (Schema schema : types) {
            unionArray.add(schema.toJson());
          }
        }
        return unionArray;

      case FIXED:
        node.put("type", "fixed");
        node.put("name", name);
        if (namespace != null) node.put("namespace", namespace);
        if (doc != null) node.put("doc", doc);
        node.put("size", fixedSize);
        break;

      default:
        return MAPPER.valueToTree(type.getName());
    }

    return node;
  }

  // Getter methods
  public Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getDoc() {
    return doc;
  }

  public String getFullName() {
    return fullName;
  }

  public List<Field> getFields() {
    return fields != null ? new ArrayList<>(fields) : null;
  }

  public List<String> getEnumSymbols() {
    return symbols != null ? new ArrayList<>(symbols) : null;
  }

  public Schema getElementType() {
    return elementType;
  }

  public Schema getValueType() {
    return valueType;
  }

  public List<Schema> getTypes() {
    return types != null ? new ArrayList<>(types) : null;
  }

  public int getFixedSize() {
    return fixedSize;
  }

  public Field getField(String name) {
    if (fields == null) return null;
    for (Field field : fields) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  public void setFields(List<Field> fields) {
    if (this.fields != null) {
      this.fields.clear();
      this.fields.addAll(fields);
      // Update field positions
      for (int i = 0; i < this.fields.size(); i++) {
        this.fields.get(i).setPos(i);
      }
    }
  }

  public void addAlias(String alias) {
    aliases.add(alias);
  }

  public Set<String> getAliases() {
    return new HashSet<>(aliases);
  }

  public void addProp(String name, Object value) {
    props.put(name, value);
  }

  public Object getProp(String name) {
    return props.get(name);
  }

  public String getProp(String name, String defaultValue) {
    Object value = props.get(name);
    return value != null ? value.toString() : defaultValue;
  }

  public Map<String, Object> getObjectProps() {
    return new HashMap<>(props);
  }

  public LogicalType getLogicalType() {
    return logicalType;
  }

  public void setLogicalType(LogicalType logicalType) {
    this.logicalType = logicalType;
    if (logicalType != null) {
      logicalType.validate(this);
    }
  }

  private String computeFullName(String name, String namespace) {
    if (name == null) return null;
    if (namespace == null || name.contains(".")) return name;
    return namespace + "." + name;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;

    Schema schema = (Schema) obj;
    return Objects.equals(toString(), schema.toString());
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
