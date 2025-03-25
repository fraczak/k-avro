package org.fraczak.k.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.MediaType;

import java.io.IOException;

@RestController
@RequestMapping("/api")
public class CsmToAvroController {

    @PostMapping("/csm-to-avro")
    public ResponseEntity<String> convertAllToAvro(@RequestBody String csmJson) throws IOException {
        JsonNode csmMap = Libs.objectMapper.readTree(csmJson);
        ObjectNode allSchemas = Libs.objectMapper.createObjectNode();
        allSchemas.put("protocol", "KProtocol");
        allSchemas.put("namespace", "k.avro");
        ArrayNode allTypes = Libs.objectMapper.createArrayNode();
        csmMap.fieldNames().forEachRemaining(typeName -> 
            allTypes.add(Libs.csmToAvro(typeName, csmMap.get(typeName)))
        );
        // System.out.println(allTypes.toPrettyString());
        allSchemas.set("types",allTypes);

        System.out.println(allSchemas.toString());
        
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(allSchemas.toString());
    }

    @PostMapping("/avro-to-csm")
    public ResponseEntity<String> convertAvroToCsm(@RequestBody String avroJson) throws IOException {
        JsonNode avroSchemas = Libs.objectMapper.readTree(avroJson);
        ObjectNode csmMap = Libs.objectMapper.createObjectNode();
        ArrayNode allTypes = (ArrayNode) avroSchemas.get("types");
        allTypes.forEach(typeNode -> {
            String typeName = typeNode.get("name").asText(); 
            ObjectNode theField = (ObjectNode) typeNode.get("fields").get(0);
            csmMap.set(typeName, Libs.avroToCsm(theField));
        });
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(csmMap.toString());
    }
}
