package org.fraczak.k.avro;

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
            throw new IllegalArgumentException("Unsupported CSM type: " + typeName);
        }

        // System.out.println(avroSchema.toPrettyString());

        return avroSchema;
    }

    public static JsonNode avroToCsm(JsonNode avroSchema) {
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
        } else if (theType.isTextual() && "array".equals(theType.asText())) {
            ObjectNode product = objectMapper.createObjectNode();
            String itemType = avroSchema.get("items").asText();
            csmType.put("vector", itemType);
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
