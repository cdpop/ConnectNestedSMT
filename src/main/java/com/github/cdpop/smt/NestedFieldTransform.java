package com.github.cdpop.smt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NestedFieldTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(NestedFieldTransform.class);

    private static final String DELIMITER_CONFIG = "delimiter";
    private String delimiter;

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }

        Struct value = (Struct) record.value();
        Map<String, Object> nestedMap = new HashMap<>();

        for (Field field : value.schema().fields()) {
            String[] parts = field.name().split(delimiter);
            if (parts.length == 1) {
                // Field is not nested, add directly to the map
                addNonNestedField(nestedMap, field.name(), field.schema(), value.get(field.name()));
            } else {
                // Field is nested, process it recursively
                addFieldToMap(nestedMap, parts, 0, field.schema(), value.get(field.name()));
            }
        }

        // Create schema directly from the map
        Schema schema = createSchemaBuilder(nestedMap).build();
        log.info("Final Schema:");
        printSchemaFields(schema, "");
        log.info("\nFinal Map:");
        printMap(nestedMap, "");
        Struct newValue = buildStruct(schema, nestedMap);

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), schema, newValue, record.timestamp());
    }

    private void addFieldToMap(Map<String, Object> map, String[] parts, int index, Schema fieldSchema, Object fieldValue) {
        String part = parts[index];
        boolean isLastPart = index == parts.length - 1;

        if (isLastPart) {
            // Add the field value to the map
            map.put(part, fieldValue);
        } else {
            // Retrieve or create the nested map
            Map<String, Object> nestedMap = (Map<String, Object>) map.computeIfAbsent(part, k -> new HashMap<>());
            // Recursively add the nested field
            addFieldToMap(nestedMap, parts, index + 1, fieldSchema, fieldValue);
        }
    }

    private void addNonNestedField(Map<String, Object> map, String fieldName, Schema fieldSchema, Object fieldValue) {
        map.put(fieldName, fieldValue);
    }

    public static SchemaBuilder createSchemaBuilder(Map<String, Object> map) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                schemaBuilder.field(key, Schema.STRING_SCHEMA);
            } else if (value instanceof Integer) {
                schemaBuilder.field(key, Schema.INT32_SCHEMA);
            } else if (value instanceof Boolean) {
                schemaBuilder.field(key, Schema.BOOLEAN_SCHEMA);
            } else if (value instanceof Long) {
                schemaBuilder.field(key, Schema.INT64_SCHEMA);
            } else if (value instanceof Double) {
                schemaBuilder.field(key, Schema.FLOAT64_SCHEMA);
            } else if (value instanceof Float) {
                schemaBuilder.field(key, Schema.FLOAT32_SCHEMA);
            } else if (value instanceof Short) {
                schemaBuilder.field(key, Schema.INT16_SCHEMA);
            } else if (value instanceof Byte) {
                schemaBuilder.field(key, Schema.INT8_SCHEMA);
            } else if (value instanceof byte[]) {
                schemaBuilder.field(key, Schema.BYTES_SCHEMA);
            } else if (value instanceof ByteBuffer) {
                schemaBuilder.field(key, Schema.BYTES_SCHEMA);
            } else if (value instanceof List) {
                // Assuming a homogeneous list and using the first element to determine the schema
                Object firstElement = ((List<?>) value).isEmpty() ? null : ((List<?>) value).get(0);
                if (firstElement instanceof Integer) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.INT32_SCHEMA).build());
                } else if (firstElement instanceof String) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.STRING_SCHEMA).build());
                } else if (firstElement instanceof Boolean) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build());
                } else if (firstElement instanceof Long) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.INT64_SCHEMA).build());
                } else if (firstElement instanceof Double) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build());
                } else if (firstElement instanceof Float) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build());
                } else if (firstElement instanceof Short) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.INT16_SCHEMA).build());
                } else if (firstElement instanceof Byte) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.INT8_SCHEMA).build());
                } else if (firstElement instanceof byte[]) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.BYTES_SCHEMA).build());
                } else if (firstElement instanceof ByteBuffer) {
                    schemaBuilder.field(key, SchemaBuilder.array(Schema.BYTES_SCHEMA).build());
                } else {
                    throw new IllegalArgumentException("Unsupported list type: " + firstElement.getClass());
                }
            } else if (value instanceof Map) {
                // Recursively create a nested schema for the map
                schemaBuilder.field(key, createSchemaBuilder((Map<String, Object>) value).build());
            } else if (value instanceof java.sql.Timestamp) {
                schemaBuilder.field(key, Timestamp.SCHEMA);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + value.getClass());
            }
        }

        return schemaBuilder;
    }

    private Struct buildStruct(Schema schema, Map<String, Object> map) {
        Struct struct = new Struct(schema);

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            Schema fieldSchema = schema.field(fieldName).schema();

            // Check if the field value is a nested Map (complex type)
            if (fieldValue instanceof Map) {
                Struct nestedStruct = buildStruct(fieldSchema, (Map<String, Object>) fieldValue);
                struct.put(fieldName, nestedStruct);

            // Check if the field value is a Timestamp
            } else if (fieldValue instanceof java.sql.Timestamp) {
                struct.put(fieldName, fieldValue); // Use Timestamp object directly

            // Handle primitive data types
            } else {
                // Attempt to convert the field value to the expected schema type
                Object convertedValue = convertToSchemaType(fieldSchema, fieldValue);
                if (convertedValue != null) {
                    struct.put(fieldName, convertedValue);
                } else {
                    log.warn("Skipping field '{}' due to type mismatch. Expected: '{}', but got: '{}'", 
                             fieldName, fieldSchema.type(), fieldValue.getClass().getSimpleName());
                }
            }
        }

        return struct;
    }

    private Object convertToSchemaType(Schema fieldSchema, Object fieldValue) {
        try {
            log.info("Pops fieldSchema: " + fieldSchema.name() + " type: " + fieldSchema.type());
            switch (fieldSchema.type()) {
                case STRING:
                    return fieldValue.toString();
                case INT8:
                    if (fieldValue instanceof Number) {
                        return ((Number) fieldValue).byteValue();
                    } else if (fieldValue instanceof String) {
                        return Byte.parseByte((String) fieldValue);
                    }
                    break;
                case INT16:
                    if (fieldValue instanceof Number) {
                        return ((Number) fieldValue).shortValue();
                    } else if (fieldValue instanceof String) {
                        return Short.parseShort((String) fieldValue);
                    }
                    break;
                case INT32:
                    if (fieldValue instanceof Number) {
                        log.info("Pops fieldValue int 32: " + fieldValue);
                        return ((Number) fieldValue).intValue();
                    } else if (fieldValue instanceof String) {
                        log.info("Pops fieldValue int 32: " + fieldValue);
                        return Integer.parseInt((String) fieldValue);
                    }
                    break;
                case INT64:
                    if (fieldValue instanceof Number) {
                        return ((Number) fieldValue).longValue();
                    } else if (fieldValue instanceof String) {
                        return Long.parseLong((String) fieldValue);
                    }
                    break;
                case FLOAT32:
                    if (fieldValue instanceof Number) {
                        return ((Number) fieldValue).floatValue();
                    } else if (fieldValue instanceof String) {
                        return Float.parseFloat((String) fieldValue);
                    }
                    break;
                case FLOAT64:
                    if (fieldValue instanceof Number) {
                        return ((Number) fieldValue).doubleValue();
                    } else if (fieldValue instanceof String) {
                        return Double.parseDouble((String) fieldValue);
                    }
                    break;
                case BOOLEAN:
                    if (fieldValue instanceof Boolean) {
                        return fieldValue;
                    } else if (fieldValue instanceof String) {
                        return Boolean.parseBoolean((String) fieldValue);
                    }
                    break;
                case BYTES:
                    if (fieldValue instanceof ByteBuffer) {
                        return fieldValue;
                    } else if (fieldValue instanceof byte[]) {
                        return ByteBuffer.wrap((byte[]) fieldValue);
                    }
                    break;
                // Add more cases for other primitive types if needed
                default:
                    log.warn("Unsupported schema type '{}' for field value '{}'", 
                             fieldSchema.type(), fieldValue);
                    return null;
            }
        } catch (Exception e) {
            log.warn("Failed to convert value '{}' to schema type '{}': {}", 
                     fieldValue, fieldSchema.type(), e.getMessage());
        }

        return null; // Return null if conversion fails
    }

    // Method to print schema fields and their data types
    private void printSchemaFields(Schema schema, String indent) {
        for (Field field : schema.fields()) {
            log.info("{}Field Name: '{}', Type: '{}'", indent, field.name(), field.schema().type());
            if (field.schema().type() == Schema.Type.STRUCT) {
                log.info("{}Entering nested struct: '{}'", indent, field.name());
                printSchemaFields(field.schema(), indent + "  ");
            }
        }
    }

    // Method to print the map with keys and values
    private void printMap(Map<String, Object> map, String indent) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                log.info("{}Key: '{}', Value: (nested map)", indent, entry.getKey());
                printMap((Map<String, Object>) entry.getValue(), indent + "  ");
            } else {
                log.info("{}Key: '{}', Value: '{}'", indent, entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(DELIMITER_CONFIG, ConfigDef.Type.STRING, ".", ConfigDef.Importance.HIGH, "Delimiter for nested fields.");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(config(), configs);
        this.delimiter = config.getString(DELIMITER_CONFIG);
    }

    @Override
    public void close() {
        // No-op
    }
}
