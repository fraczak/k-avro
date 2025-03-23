# README

```bash
  mvn clean package -U
  mvn spring-boot:run
```

```bash
  curl -v "http://localhost:8080/csm-to-avro" \
    -H "Content-Type: application/json" \
    -d '{
          "BsAqRMv": {
            "union": {
              "0": "BsAqRMv",
              "1": "BsAqRMv",
              "_": "KL"
            }
          },
          "KL": {
            "product": {}
          }
        }'
```

```bash
curl  "http://localhost:8080/avro-to-csm" -H "Content-Type: application/json" \
  -d '{
    "KL":{"name":"KL","type":"record","fields":[]},
    "BsAqRMv":
      { "name":"BsAqRMv",
        "type": [
          {"name":"BsAqRMv_30","type":"record","fields":[{"name":"H_30","type":"BsAqRMv"}]},
          {"name":"BsAqRMv_31","type":"record","fields":[{"name":"H_31","type":"BsAqRMv"}]},
          {"name":"BsAqRMv_5f","type":"record","fields":[{"name":"H_5f","type":"KL"}]}
        ]
      }
    }'
```
