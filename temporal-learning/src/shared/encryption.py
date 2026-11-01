"""Phase 14: encrypt payloads before they leave your process.

With this codec the Temporal Service stores ciphertext it cannot read, so the keys
stay in your network. Key IDs travel with the data, which makes rotation possible:
new data uses the active key, and old history stays readable with the old keys.
"""

import dataclasses
import os
from collections.abc import Sequence

import temporalio.converter
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter, DefaultFailureConverterWithEncodedAttributes, PayloadCodec

ENCODING = b"binary/encrypted"


class EncryptionCodec(PayloadCodec):
    def __init__(self, keys: dict[str, bytes], active_key_id: str) -> None:
        self.ciphers = {key_id: AESGCM(key) for key_id, key in keys.items()}
        self.active_key_id = active_key_id

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [self._encrypt(payload) for payload in payloads]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            self._decrypt(payload) if payload.metadata.get("encoding") == ENCODING else payload
            for payload in payloads
        ]

    def _encrypt(self, payload: Payload) -> Payload:
        # A nonce must never repeat under one key, so each payload gets a fresh random one.
        nonce = os.urandom(12)
        secret = self.ciphers[self.active_key_id].encrypt(nonce, payload.SerializeToString(), None)
        return Payload(
            metadata={"encoding": ENCODING, "key-id": self.active_key_id.encode()},
            data=nonce + secret,
        )

    def _decrypt(self, payload: Payload) -> Payload:
        key_id = payload.metadata["key-id"].decode()
        if key_id not in self.ciphers:
            raise ValueError(f"unknown encryption key: {key_id}")
        nonce, secret = payload.data[:12], payload.data[12:]
        return Payload.FromString(self.ciphers[key_id].decrypt(nonce, secret, None))


def encrypted_converter(codec: PayloadCodec) -> DataConverter:
    return dataclasses.replace(
        temporalio.converter.default(),
        payload_codec=codec,
        # Without this, failure messages and stack traces are stored in plain text.
        failure_converter_class=DefaultFailureConverterWithEncodedAttributes,
    )
