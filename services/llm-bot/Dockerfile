FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/data/hf \
    SENTENCE_TRANSFORMERS_HOME=/data/hf

WORKDIR /app

# System deps for sentence-transformers / chromadb wheels.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app ./app
COPY data ./data
COPY seed_rag.py .

# Generate gRPC stubs from the proto file bundled with the grpc package.
# grpcio-tools is in requirements.txt and available at build time.
# The -I flag and source path together determine the output module path:
#   -I /app  + source app/grpc/bot.proto  →  output at app/grpc/bot_pb2*.py
RUN python -m grpc_tools.protoc \
    -I /app \
    --python_out=/app \
    --grpc_python_out=/app \
    /app/app/grpc/bot.proto && \
    # Fix the import in the generated _grpc file so it resolves as a package import.
    sed -i 's/^import bot_pb2/from app.grpc import bot_pb2/' /app/app/grpc/bot_pb2_grpc.py

# Persistent volumes for chroma + model cache are mounted at /data.
RUN mkdir -p /data/chroma /data/hf

EXPOSE 8000 50051
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
