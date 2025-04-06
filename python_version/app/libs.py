from typing import Dict, Any, List
import json
from avro.schema import RecordSchema, Field, ArraySchema, UnionSchema

def to_hex(s: str) -> str:
    return ''.join([f'{b:02x}' for b in s.encode()])

def from_hex(hex_str: str) -> str:
    return bytes.fromhex(hex_str).decode()

def csm_to_avro(type_name: str, csm_type: Dict[str, Any]) -> Dict[str, Any]:
    avro_schema = {
        "name": type_name,
        "type": "record",
        "fields": [{
            "name": "union" if "union" in csm_type else "product" if "product" in csm_type else "vector",
            "type": None  # Will be set based on the type
        }]
    }
    
    if "union" in csm_type:
        union_types = []
        for tag_name, tag_type in csm_type["union"].items():
            union_type_record = {
                "name": f"{type_name}_{to_hex(tag_name)}",
                "type": "record",
                "fields": [{
                    "name": f"H_{to_hex(tag_name)}",
                    "type": tag_type
                }]
            }
            union_types.append(union_type_record)
        avro_schema["fields"][0]["type"] = union_types
        
    elif "product" in csm_type:
        product_type = {
            "name": f"{type_name}_record",
            "type": "record",
            "fields": []
        }
        
        for tag_name, tag_type in csm_type["product"].items():
            product_type["fields"].append({
                "name": f"H_{to_hex(tag_name)}",
                "type": tag_type
            })
        
        avro_schema["fields"][0]["type"] = product_type
        
    elif "vector" in csm_type:
        vector_type = {
            "name": f"{type_name}_vector",
            "type": "record",
            "fields": [{
                "name": "member",
                "type": csm_type["vector"]
            }]
        }
        avro_schema["fields"][0]["type"] = vector_type
        
    else:
        raise ValueError(f"Invalid CSM code: {csm_type}")
    
    return avro_schema

def avro_to_csm(type_schema: RecordSchema) -> Dict[str, Any]:
    the_field = type_schema['fields'][0]
    csm_type = {}
    if the_field['name'] == "union":
        union = {}
        for variant in the_field['type']:
            field = variant['fields'][0]
            variant_name = from_hex(field['name'][2:])
            union[variant_name] = field['type']
        csm_type["union"] = union
        
    elif the_field['name'] == "product":
        product = {}
        for field in the_field['type']['fields']:
            tag_name = from_hex(field['name'][2:])
            product[tag_name] = field['type']
        csm_type["product"] = product
        
    elif the_field['name'] == "vector":
        member_type = the_field['type']['fields'][0]['type']
        csm_type["vector"] = member_type
        
    else:
        raise ValueError(f"Unsupported Avro type: {type_schema}")
    
    return csm_type

def avro_json_to_csm(type_node: Dict[str, Any]) -> Dict[str, Any]:
    csm_type = {}
    the_field = type_node["fields"][0]
    code_type = the_field["name"]
    
    if code_type == "union":
        union = {}
        for variant in the_field["type"]:
            field = variant["fields"][0]
            variant_name = from_hex(field["name"][2:])
            union[variant_name] = field["type"]
        csm_type["union"] = union
        
    elif code_type == "product":
        product = {}
        for field in the_field["type"]["fields"]:
            tag_name = from_hex(field["name"][2:])
            product[tag_name] = field["type"]
        csm_type["product"] = product
        
    elif code_type == "vector":
        member_type = the_field["type"]["fields"][0]["type"]
        csm_type["vector"] = member_type
        
    else:
        raise ValueError(f"Unsupported Avro type: {code_type}")
    
    return csm_type 