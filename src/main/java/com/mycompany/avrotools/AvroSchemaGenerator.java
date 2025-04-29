package com.mycompany.avrotools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.data.TimeConversions;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

public class AvroSchemaGenerator {

    public static final String USER_DIR = "user.dir";

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AvroSchemaGenerator <full.class.Name> <output-file.avsc>");
            System.exit(1);
        }
        System.out.println("Current working directory: " + System.getProperty(USER_DIR));
        System.out.println("Generating Avro schema for class: " + args[0]);
        System.out.println("Current working directory: " + System.getProperty(USER_DIR));

        Class<?> cls = Class.forName(args[0]);

        for (java.lang.reflect.Field field : cls.getDeclaredFields()) {
            System.out.println("Field: " + field.getName());
        }

        // Use custom ReflectData instead of the default
        ReflectData reflectData = CustomReflectData.get();
        reflectData.addLogicalTypeConversion(new Conversions.UUIDConversion());
        reflectData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        reflectData.addLogicalTypeConversion(new Conversions.DecimalConversion());
        Schema schema = reflectData.getSchema(cls);
        // Reorder the fields in the schema string
        String reorderedSchemaStr = reorderSchemaFields(schema.toString(), cls);

        String resultJson = provideCommandDtoMetaData(reorderedSchemaStr, cls);

        // Write the result to the output file
        File out = new File(args[1]);
        try (FileWriter w = new FileWriter(out)) {
            w.write(resultJson);
        }

        System.out.println("Wrote Avro schema to " + out.getAbsolutePath());
    }

    private static String provideCommandDtoMetaData(String reorderedSchemaStr, Class<?> cls) throws JsonProcessingException {
        // Parse the reordered schema to extract its fields
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaNode = mapper.readTree(reorderedSchemaStr);
        JsonNode schemaFields = schemaNode.get("fields");

// Create the root object
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("name", cls.getName()+"Payload");
        rootNode.put("namespace", "com.bestseller.bestone.bi4.sales.transactions");
        rootNode.put("type", "record");

// Create the fields array for the root
        ArrayNode rootFields = mapper.createArrayNode();

// Add the standard fields
        ObjectNode payloadClassNameNode = mapper.createObjectNode();
        payloadClassNameNode.put("name", "payloadClassName");
        payloadClassNameNode.put("type", "string");
        rootFields.add(payloadClassNameNode);

        ObjectNode payloadKeyNode = mapper.createObjectNode();
        payloadKeyNode.put("name", "payloadKey");
        payloadKeyNode.put("type", "string");
        rootFields.add(payloadKeyNode);

        ObjectNode transactionIdNode = mapper.createObjectNode();
        transactionIdNode.put("name", "transactionId");
        transactionIdNode.put("type", "string");
        rootFields.add(transactionIdNode);

// Create the data field with nested structure
        ObjectNode dataNode = mapper.createObjectNode();
        dataNode.put("name", "data");

// Create data type object
        ObjectNode dataTypeNode = mapper.createObjectNode();
        dataTypeNode.put("type", "record");
        dataTypeNode.set("fields", schemaFields);
// You can copy other fields from the original schema if needed
        if (schemaNode.has("name")) {
            dataTypeNode.put("name", schemaNode.get("name").asText());
        }
        if (schemaNode.has("namespace")) {
            dataTypeNode.put("namespace", schemaNode.get("namespace").asText());
        }

        dataNode.set("type", dataTypeNode);
        rootFields.add(dataNode);

// Add the fields array to the root object
        rootNode.set("fields", rootFields);

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
    }

    // here we convert String type to avro.java.string
    private static class CustomReflectData extends ReflectData {
        private static final CustomReflectData INSTANCE = new CustomReflectData();

        public static CustomReflectData get() {
            return INSTANCE;
        }

        @Override
        protected Schema createSchema(Type type, Map<String, Schema> names) {
            // Handle String type
            if (type == String.class) {
                Schema stringSchema = Schema.create(Schema.Type.STRING);
                stringSchema.addProp("avro.java.string", "String");
                return stringSchema;
            }

            // Handle Timestamp types
            if (type == java.util.Date.class ||
                    type == java.sql.Timestamp.class) {

                Schema timestampSchema = Schema.create(Schema.Type.LONG);
                timestampSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp");
                timestampSchema.addProp("connect.version", "1");
                timestampSchema.addProp("logicalType", "timestamp-millis");
                return timestampSchema;
            }

            return super.createSchema(type, names);
        }

    }

    private static String reorderSchemaFields(String schemaJson, Class<?> cls) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(schemaJson);

        if (!rootNode.has("type") || !rootNode.get("type").asText().equals("record")) {
            return schemaJson;
        }

        Map<String, JsonNode> fieldMap = new LinkedHashMap<>();
        JsonNode fieldsNode = rootNode.get("fields");
        for (JsonNode field : fieldsNode) {
            fieldMap.put(field.get("name").asText(), field);
        }

        ArrayNode newFields = mapper.createArrayNode();
        Field[] declaredFields = cls.getDeclaredFields();

        for (Field field : declaredFields) {
            if ((field.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0) {
                continue;
            }

            String fieldName = field.getName();
            if (fieldMap.containsKey(fieldName)) {
                newFields.add(fieldMap.get(fieldName));
                fieldMap.remove(fieldName);
            }
        }

        for (JsonNode remainingField : fieldMap.values()) {
            newFields.add(remainingField);
        }

        ((ObjectNode) rootNode).set("fields", newFields);

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
    }



}