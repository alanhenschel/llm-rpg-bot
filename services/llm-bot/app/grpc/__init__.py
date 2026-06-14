# Allow generated gRPC stubs (which emit bare `import bot_pb2`) to resolve
# correctly when imported as `from app.grpc import bot_pb2`.
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parent))
