package org.conflictbits;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Libs {

    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode csmToAvro(String typeName, JsonNode csmMap) {
        ObjectNode avroSchema = objectMapper.createObjectNode();
        avroSchema.put("name", typeName);

        JsonNode csmType = csmMap.get(typeName);
        if (csmType.has("product")) {
            ArrayNode fields = objectMapper.createArrayNode();
            JsonNode product = csmType.get("product");
            product.fieldNames().forEachRemaining(tagName -> {
                ObjectNode field = objectMapper.createObjectNode();
                field.put("name", "H_" + toHex(tagName));
                field.put("type", product.get(tagName).asText());
                fields.add(field);
            });
            avroSchema.put("type", "record");
            avroSchema.set("fields", fields);
        } else if (csmType.has("union")) {
            ArrayNode unionArray = objectMapper.createArrayNode();
            JsonNode union = csmType.get("union");
            union.fieldNames().forEachRemaining(tagName -> {
                String fieldName = toHex(tagName);
                ObjectNode variantRecord = objectMapper.createObjectNode();
                variantRecord.put("name", typeName + "_" + fieldName);
                variantRecord.put("type", "record");
                ArrayNode variantFields = objectMapper.createArrayNode();
                ObjectNode variantField = objectMapper.createObjectNode();
                variantField.put("name", "H_" + fieldName);
                variantField.put("type", union.get(tagName).asText());
                variantFields.add(variantField);
                variantRecord.set("fields", variantFields);
                unionArray.add(variantRecord);
            });
            avroSchema.set("type", unionArray);
        } else {
            throw new IllegalArgumentException("Unsupported CSM type: " + typeName);
        }
        return avroSchema;
    }

    public static JsonNode avroToCsm(String typeName, JsonNode avroSchema) {
        ObjectNode csmType = objectMapper.createObjectNode();

        JsonNode theType = avroSchema.get("type");
        if (theType.isTextual() && "record".equals(theType.asText())) {
            ObjectNode product = objectMapper.createObjectNode();
            ArrayNode fields = (ArrayNode) avroSchema.get("fields");
            fields.forEach(field -> {
                String tagName = fromHex(field.get("name").asText().substring(2));
                product.put(tagName, field.get("type").asText());
            });
            csmType.set("product", product);
        } else if (theType.isArray()) {
            ArrayNode unionArray = (ArrayNode) theType;
            ObjectNode union = objectMapper.createObjectNode();
            unionArray.forEach(variant -> {
                JsonNode variantField = variant.get("fields").get(0);
                String fieldName = fromHex(variantField.get("name").asText().substring(2));
                union.put(fieldName, variantField.get("type").asText());
            });
            csmType.set("union", union);
        } else {
            throw new IllegalArgumentException("Unsupported Avro type: " + avroSchema.get("type").asText());
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
