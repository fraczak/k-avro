# K-Avro Service (Python Version)

This is a Python 3 version of the K-Avro service that converts between CSM (Custom Schema Model) and Avro formats.

## Setup

1. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Service

To run the service:

```bash
cd app
uvicorn main:app --host 0.0.0.0 --port 8000
```

The service will be available at `http://localhost:8000`

## API Endpoints

### 1. CSM to Avro Conversion
- **Endpoint**: `/api/csm-to-avro`
- **Method**: POST
- **Content-Type**: application/json
- **Request Body**: CSM JSON string
- **Response**: Avro schema JSON

### 2. Avro to CSM Conversion
- **Endpoint**: `/api/avro-to-csm`
- **Method**: POST
- **Content-Type**: application/json
- **Request Body**: Avro schema JSON string
- **Response**: CSM JSON

### 3. Avro JSON to CSM Conversion
- **Endpoint**: `/api/avroJson-to-csm`
- **Method**: POST
- **Content-Type**: application/json
- **Request Body**: Avro JSON schema string
- **Response**: CSM JSON

## API Documentation

Once the service is running, you can access the interactive API documentation at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Example Usage

```python
import requests
import json

# Example CSM to Avro conversion
csm_data = {
    "MyType": {
        "product": {
            "field1": "string",
            "field2": "int"
        }
    }
}

response = requests.post(
    "http://localhost:8000/api/csm-to-avro",
    json=csm_data
)
avro_schema = response.json()

# Example Avro to CSM conversion
avro_data = {
    "protocol": "KProtocol",
    "namespace": "k.avro",
    "types": [
        {
            "name": "MyType",
            "type": "record",
            "fields": [
                {
                    "name": "product",
                    "type": {
                        "name": "MyType_record",
                        "type": "record",
                        "fields": [
                            {"name": "H_6669656c6431", "type": "string"},
                            {"name": "H_6669656c6432", "type": "int"}
                        ]
                    }
                }
            ]
        }
    ]
}

response = requests.post(
    "http://localhost:8000/api/avro-to-csm",
    json=avro_data
)
csm_schema = response.json()
``` 

---

```text
AI Prompt

I need to create a Python 3 version of a Java microservice that converts between CSM (Custom Schema Model) and Avro formats. The service should:

1. Use FastAPI as the web framework
2. Implement three endpoints:
   - /api/csm-to-avro: Convert CSM format to Avro
   - /api/avro-to-csm: Convert Avro format to CSM
   - /api/avroJson-to-csm: Convert Avro JSON format to CSM

The service should handle:
- Union types
- Product types
- Vector types
- Hex encoding/decoding of field names
- Proper error handling and logging

The implementation should:
1. Use modern Python features and best practices
2. Include proper type hints
3. Handle JSON requests/responses appropriately
4. Include comprehensive documentation
5. Be compatible with Python 3.11 (stable version)

The service should maintain the same functionality as the Java version, including:
- Protocol and namespace handling
- Field name encoding/decoding
- Type conversion logic
- Error handling and logging
```
