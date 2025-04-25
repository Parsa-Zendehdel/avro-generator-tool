package com.mycompany.avrotools;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.data.TimeConversions;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Type;
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
        File out = new File(args[1]);
        try (FileWriter w = new FileWriter(out)) {
            w.write(schema.toString(true));
        }
        System.out.println("Wrote Avro schema to " + out.getAbsolutePath());
    }

    // here we convert String type to avro.java.string
    private static class CustomReflectData extends ReflectData {
        private static final CustomReflectData INSTANCE = new CustomReflectData();

        public static CustomReflectData get() {
            return INSTANCE;
        }

        @Override
        protected Schema createSchema(Type type, Map<String, Schema> names) {
            if (type == String.class) {
                // Create a custom schema for String type
                Schema stringSchema = Schema.create(Schema.Type.STRING);
                stringSchema.addProp("avro.java.string", "String");
                return stringSchema;
            }
            return super.createSchema(type, names);
        }
    }

}