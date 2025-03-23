package org.conflictbits;

import com.fasterxml.jackson.databind.JsonNode;
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
        csmMap.fieldNames().forEachRemaining(typeName -> 
            allSchemas.set(typeName, Libs.csmToAvro(typeName, csmMap))
        );
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(allSchemas.toString());
    }

    @PostMapping("/avro-to-csm")
    public ResponseEntity<String> convertAvroToCsm(@RequestBody String avroJson) throws IOException {
        JsonNode avroSchemas = Libs.objectMapper.readTree(avroJson);
        ObjectNode csmMap = Libs.objectMapper.createObjectNode();
        avroSchemas.fieldNames().forEachRemaining(typeName -> 
            csmMap.set(typeName, Libs.avroToCsm(typeName, avroSchemas.get(typeName)))
        );
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(csmMap.toString());
    }
}
