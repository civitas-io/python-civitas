"""Generated gRPC stubs for the civitas.Agent service.

The ``*_pb2.py`` / ``*_pb2_grpc.py`` modules in this package are generated from
``civitas.proto`` and committed to the repo (design Q3) so consumers need no
build-time ``protoc``. Regenerate them with::

    uv run python -m grpc_tools.protoc \
        -I civitas/gateway/proto \
        --python_out=civitas/gateway/proto \
        --grpc_python_out=civitas/gateway/proto \
        civitas/gateway/proto/civitas.proto

then re-apply the package-relative import fix in ``civitas_pb2_grpc.py``.
"""
