package org.fraczak.k.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.apache.avro.Protocol;
import org.apache.avro.SchemaParseException;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.util.logging.Logger;

@RestController
@RequestMapping("/api")
public class CsmToAvroController {

    private static final Logger logger = Logger.getLogger(CsmToAvroController.class.getName());

    @PostMapping("/csm-to-avro")
    public ResponseEntity<String> convertCsmToAvro(@RequestBody String csmJson) throws IOException {
        logger.info("CSM->AVRO --- INPUT: " + csmJson);
        
        JsonNode csmMap = Libs.objectMapper.readTree(csmJson);

        ObjectNode allSchemas = Libs.objectMapper.createObjectNode();
        allSchemas.put("protocol", "KProtocol");
        allSchemas.put("namespace", "k.avro");
        ArrayNode allTypes = Libs.objectMapper.createArrayNode();
        csmMap.fieldNames().forEachRemaining(typeName -> 
            allTypes.add(Libs.csmToAvro(typeName, csmMap.get(typeName)))
        );
        
        allSchemas.set("types",allTypes);

        logger.info("          -- OUTPUT: " + allSchemas.toString());
        
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(allSchemas.toString());
    }

    @PostMapping("/avro-to-csm")
    public ResponseEntity<String> convertAvroToCsm(@RequestBody String avroJson) throws IOException {
        logger.info("AVRO->CSM --- INPUT: " + avroJson);

        ObjectNode csmMap = Libs.objectMapper.createObjectNode();
        Protocol protocol;

        try {
            protocol = Protocol.parse(avroJson);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Invalid Avro schema: " + e.getMessage());
        }

        protocol.getTypes().forEach(type -> {
            String typeName = type.getName();
            if (typeName.contains("_")) {
                return;
            }
            csmMap.set(typeName, Libs.avroToCsm(type));
        });

        logger.info("          -- OUTPUT: " + csmMap.toString());
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(csmMap.toString());
    }

    @PostMapping("/avroJson-to-csm")
    public ResponseEntity<String> convertAvroJsonToCsm(@RequestBody String avroJson) throws IOException {
        logger.info("AVRO->CSM --- INPUT: " + avroJson);

        ObjectNode csmMap = Libs.objectMapper.createObjectNode();
       
        JsonNode avroSchemas = Libs.objectMapper.readTree(avroJson);        
        ArrayNode allTypes = (ArrayNode) avroSchemas.get("types");

        allTypes.forEach(typeNode -> {
            String typeName = typeNode.get("name").asText();
            csmMap.set(typeName, Libs.avroJsonToCsm(typeNode));
        });

        logger.info("          -- OUTPUT: " + csmMap.toString());
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(csmMap.toString());
    }
}
