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
        Protocol protocol;
        try {
            protocol = Protocol.parse(avroJson);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Invalid Avro schema: " + e.getMessage());
        }
        JsonNode avroSchemas = Libs.objectMapper.readTree(avroJson);

        ObjectNode csmMap = Libs.objectMapper.createObjectNode();
        ArrayNode allTypes = (ArrayNode) avroSchemas.get("types");

        // protocol.getTypes().forEach(type -> {
        //     String typeName = type.getName();
        //     csmMap.set(typeName, Libs.avroToCsm2(type));
        // });

        allTypes.forEach(typeNode -> {
            String typeName = typeNode.get("name").asText(); 
            ObjectNode theField = (ObjectNode) typeNode.get("fields").get(0);
            csmMap.set(typeName, Libs.avroToCsm(theField));
        });

        logger.info("          -- OUTPUT: " + csmMap.toString());
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(csmMap.toString());
    }
}
