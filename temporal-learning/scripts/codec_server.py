"""Phase 14: lets the Web UI show decrypted payloads to authorised people.

The browser sends ciphertext here and gets plaintext back, so the Temporal Service
never sees your keys. This server can read everything, so protect it like one.

    set CODEC_TOKEN and CODEC_KEY_BASE64, then:
    uvicorn scripts.codec_server:app --port 8081
"""

import base64
import os

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from google.protobuf import json_format
from temporalio.api.common.v1 import Payloads

from shared.encryption import EncryptionCodec


def build_app(codec: EncryptionCodec, token: str, ui_origin: str = "http://localhost:8233") -> FastAPI:
    app = FastAPI()
    app.add_middleware(CORSMiddleware, allow_origins=[ui_origin], allow_methods=["POST"], allow_headers=["*"])

    def authorised(authorization: str = Header(default="")) -> None:
        if authorization != f"Bearer {token}":
            raise HTTPException(status_code=401, detail="unauthorised")

    async def convert(request: Request, operation) -> dict:
        payloads = json_format.ParseDict(await request.json(), Payloads())
        return json_format.MessageToDict(Payloads(payloads=await operation(payloads.payloads)))

    @app.post("/decode", dependencies=[Depends(authorised)])
    async def decode(request: Request) -> dict:
        return await convert(request, codec.decode)

    @app.post("/encode", dependencies=[Depends(authorised)])
    async def encode(request: Request) -> dict:
        return await convert(request, codec.encode)

    return app


if os.environ.get("CODEC_KEY_BASE64"):
    app = build_app(
        EncryptionCodec({"key-1": base64.b64decode(os.environ["CODEC_KEY_BASE64"])}, "key-1"),
        os.environ["CODEC_TOKEN"],
    )
