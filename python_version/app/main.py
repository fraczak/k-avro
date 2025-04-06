from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
from typing import Dict, Any
from avro.schema import parse, RecordSchema, Field
from .libs import csm_to_avro, avro_to_csm, avro_json_to_csm
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="K-Avro Service")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define request models
class CsmToAvroRequest(BaseModel):
    __root__: Dict[str, Any]

class AvroToCsmRequest(BaseModel):
    __root__: Dict[str, Any]

@app.post("/api/csm-to-avro")
async def convert_csm_to_avro(request: CsmToAvroRequest):
    try:
        csm_map = request.__root__
        logger.info(f"CSM->AVRO --- INPUT: {json.dumps(csm_map)}")
        
        all_schemas = {
            "protocol": "KProtocol",
            "namespace": "k.avro",
            "types": []
        }
        
        for type_name, csm_type in csm_map.items():
            all_schemas["types"].append(csm_to_avro(type_name, csm_type))
        
        logger.info(f"          -- OUTPUT: {json.dumps(all_schemas)}")
        return JSONResponse(content=all_schemas)
    except Exception as e:
        logger.error(f"Error converting CSM to Avro: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/avro-to-csm")
async def convert_avro_to_csm(request: AvroToCsmRequest):
    try:
        avro_json = json.dumps(request.__root__)
        logger.info(f"AVRO->CSM --- INPUT: {avro_json}")
        
        # protocol = parse(avro_json)

        protocol = json.loads(avro_json)
        csm_map = {}
        
        for type_schema in protocol['types']:
            type_name = type_schema['name']
            if "_" not in type_name:
                csm_map[type_name] = avro_to_csm(type_schema)
        
        logger.info(f"          -- OUTPUT: {json.dumps(csm_map)}")
        return JSONResponse(content=csm_map)
    except Exception as e:
        logger.error(f"Error converting Avro to CSM: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/avroJson-to-csm")
async def convert_avro_json_to_csm(request: AvroToCsmRequest):
    try:
        avro_schemas = request.__root__
        logger.info(f"AVRO->CSM --- INPUT: {json.dumps(avro_schemas)}")
        
        csm_map = {}
        
        for type_node in avro_schemas["types"]:
            type_name = type_node["name"]
            csm_map[type_name] = avro_json_to_csm(type_node)
        
        logger.info(f"          -- OUTPUT: {json.dumps(csm_map)}")
        return JSONResponse(content=csm_map)
    except Exception as e:
        logger.error(f"Error converting Avro JSON to CSM: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 