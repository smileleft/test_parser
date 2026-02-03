# test_parser

## how to run

```bash
# install package
pip install -r requirements.txt

# OpenTelemetry Collector
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4317"
export OTEL_SERVICE_NAME="secs-udp-ingestor"

# PostgreSQL DSN
export PG_DSN="postgresql://postgres:postgres@localhost:5432/postgres"

# UDP port
export UDP_PORT=5000

# run
python3 app.py
```
