package org.fraczak.k.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Libs {

    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode csmToAvro(String typeName, JsonNode csmType) {
        ObjectNode avroSchema = Libs.objectMapper.createObjectNode();
        
        avroSchema.put("name",typeName);
        avroSchema.put("type","record");

        ArrayNode fields = objectMapper.createArrayNode();
        ObjectNode theField = objectMapper.createObjectNode();
        fields.add(theField);
        avroSchema.set("fields",fields);

        if (csmType.has("union")) {
            theField.put("name","union");
            ArrayNode unionType = objectMapper.createArrayNode();

            JsonNode union = csmType.get("union");
            union.fieldNames().forEachRemaining(tagName -> {
                ObjectNode unionTypeRecord = objectMapper.createObjectNode();
                unionTypeRecord.put("name", typeName + "_" + toHex(tagName));
                unionTypeRecord.put("type", "record");
                ArrayNode unionRecordFields = objectMapper.createArrayNode();
                ObjectNode unionRecordField = objectMapper.createObjectNode();
                unionRecordField.put("name","H_" + toHex(tagName));
                unionRecordField.put("type", union.get(tagName).asText());
                unionRecordFields.add(unionRecordField);
                unionTypeRecord.set("fields",unionRecordFields);
                unionType.add(unionTypeRecord);
            });

            theField.set("type",unionType);

        } else if (csmType.has("product")) {
            theField.put("name","product");

            ObjectNode productType = Libs.objectMapper.createObjectNode();
            productType.put("name", typeName + "_record");
            productType.put("type", "record");

            ArrayNode productTypeFields = Libs.objectMapper.createArrayNode();

            JsonNode product = csmType.get("product");
            product.fieldNames().forEachRemaining(tagName -> {
                ObjectNode fieldSpec = objectMapper.createObjectNode();
                fieldSpec.put("name", "H_" + toHex(tagName));
                fieldSpec.put("type", product.get(tagName).asText());
                productTypeFields.add(fieldSpec);
            });
            
            productType.set("fields",productTypeFields);
            theField.set("type",productType);

        } else if (csmType.has("vector")) {
            theField.put("name","vector");

            ObjectNode vectorType = Libs.objectMapper.createObjectNode();
            vectorType.put("name", typeName + "_vector");
            vectorType.put("type", "record");

            ArrayNode vectorTypeFields = Libs.objectMapper.createArrayNode();
            ObjectNode vectorTypeField = objectMapper.createObjectNode();
            vectorTypeField.put("name", "member");
            vectorTypeField.put("type", csmType.get("vector").asText());
            vectorTypeFields.add(vectorTypeField);
            vectorType.set("fields", vectorTypeFields);

            theField.set("type",vectorType);
        } else {
            throw new IllegalArgumentException("Invalid CSM code: " + csmType);
        }

        // System.out.println(avroSchema.toPrettyString());

        return avroSchema;
    }

    
    public static JsonNode avroToCsm(Schema type) {
        Field theField = type.getFields().get(0);

        ObjectNode csmType = objectMapper.createObjectNode();

        switch (theField.name()) {
            case "union": 
                var union = objectMapper.createObjectNode();
                var unionArray = theField.schema().getTypes();
                unionArray.forEach(variant -> {
                    Field field = variant.getFields().get(0);
                    String variantName = fromHex(field.name().substring(2));
                    union.put(variantName, field.schema().getName());
                });
                csmType.set("union", union);
                break;
            case "product":
                var product = objectMapper.createObjectNode();
                var fields = theField.schema().getFields();
                fields.forEach(field -> {
                    String tagName = fromHex(field.name().substring(2));
                    product.put(tagName, field.schema().getName());
                });
                csmType.set("product", product);
                break;
            case "vector":
                String memberType = theField.schema().getField("member").schema().getName();
                csmType.put("vector", memberType);
                break;
            default:
               throw new IllegalArgumentException("Unsupported Avro type: " + type);
        }

        return csmType;
    }

    public static JsonNode avroJsonToCsm(JsonNode typeNode) {
        ObjectNode csmType = objectMapper.createObjectNode();
         
        ObjectNode theField = (ObjectNode) typeNode.get("fields").get(0);
        String codeType = theField.get("name").asText();
        switch (codeType) {
            case "union": 
                var union = objectMapper.createObjectNode();
                var unionArray = (ArrayNode) theField.get("type");
                unionArray.forEach(variant -> {
                    ObjectNode field = (ObjectNode) variant.get("fields").get(0);
                    String variantName = fromHex(field.get("name").asText().substring(2));
                    union.put(variantName, field.get("type").asText());
                });
                csmType.set("union", union);
                break;
            case "product":
                var product = objectMapper.createObjectNode();
                var fields = (ArrayNode) theField.get("type").get("fields");
                fields.forEach(field -> {
                    String tagName = fromHex(field.get("name").asText().substring(2));
                    product.put(tagName, field.get("type").asText());
                });
                csmType.set("product", product);
                break;
            case "vector":
                String memberType = theField.get("type").get("fields").get(0).get("type").asText();
                csmType.put("vector", memberType);
                break;
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + codeType);
        }

        return csmType;
    }

    private static String toHex(String str) {
        StringBuilder encodedName = new StringBuilder();
        for (byte b : str.getBytes()) {
            encodedName.append(String.format("%02x", b));
        }
        return encodedName.toString();
    }

    private static String fromHex(String hexStr) {
        byte[] bytes = new byte[hexStr.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((Character.digit(hexStr.charAt(2 * i), 16) << 4)
                    + Character.digit(hexStr.charAt(2 * i + 1), 16));
        }
        return new String(bytes);
    }
}
