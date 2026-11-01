# Phase 14: Security and Production Architecture

## 1. Objective

Design a secure deployment and explain every trust boundary.

## 2. Mental model

Ask of every piece of data: **who can read this, at rest and in transit?**

Your history is a long-lived, detailed record of your business. Treat Temporal "similarly to a database," as the docs put it. By default, its payloads are readable by anyone with access to the Service or the UI. So you either keep sensitive data out or encrypt it before it leaves your process.

## 3. Temporal model

### Trust boundaries

```text
YOUR NETWORK                                    | TEMPORAL SERVICE
                                                | (Cloud or self-hosted)
Client ---- TLS + auth (API key or mTLS) -------+--> Frontend
Worker ---- TLS + auth (API key or mTLS) -------+--> History, Matching
  |                                             |      |
  | plaintext only here                         |      v
  | keys only here                              |    Persistence (ciphertext)
  v                                             |
Codec Server <---- browser, with auth           |
```

| Boundary | Threat | Control |
|---|---|---|
| Client or Worker to Service | Eavesdropping, impersonation | TLS, mTLS or API keys |
| Service at rest | Reading payloads | Payload Codec |
| Web UI | Staff reading data | SSO, roles, and a protected Codec Server |
| Worker | Stolen credentials | Secrets manager, least privilege |
| Environments | Test touching production | Separate namespaces and credentials |

### What the Codec encrypts, and what it doesn't

"Use a Payload Codec to encrypt payloads before they reach the Temporal Service."

| Encrypted | Not encrypted |
|---|---|
| Workflow and Activity inputs and results | **Workflow IDs** (tested) |
| Signal, Query, and Update data | Activity and Workflow type names |
| Memos | Task Queue names |
| Failure text, **only if enabled** | Search Attributes |

So never put an email address or name in a Workflow ID. `test_workflow_ids_are_not_encrypted` shows one sitting in plain view.

Failure messages and stack traces are **not** encrypted by default. This matters, because errors often quote data. `DefaultFailureConverterWithEncodedAttributes` fixes that. In the test, the stored message became `Encoded failure`.

Source: [Data encryption](https://docs.temporal.io/production-deployment/data-encryption)

### Environments

| Share between environments | Never share |
|---|---|
| Code and container images | Namespaces |
| Workflow definitions | Credentials and API keys |
| Dashboards and alert rules | Encryption keys |
| | Production data |

A staging Worker connected to production by mistake starts running production tasks. Separate credentials make that impossible.

## 4. Python implementation

Read [shared/encryption.py](../../temporal-learning/src/shared/encryption.py) and [scripts/codec_server.py](../../temporal-learning/scripts/codec_server.py).

```powershell
pytest tests/test_phase14_security.py -v
```

```python
converter = encrypted_converter(EncryptionCodec({"key-1": key}, "key-1"))
client = await Client.connect("localhost:7233", data_converter=converter)
```

| Test | Proves |
|---|---|
| `test_service_stores_only_ciphertext` | A secret name isn't in the raw history bytes |
| `test_key_rotation_keeps_old_data_readable` | Old history still decrypts after rotation |
| `test_unknown_key_is_an_error_not_garbage` | A missing key fails loudly |
| `test_tampered_data_is_rejected` | AES-GCM detects changes |
| `test_codec_server_requires_a_token_and_decodes` | The UI bridge is protected |

### Why this design

- **AES-GCM** gives secrecy and tamper detection.
- **A fresh random nonce per payload**, because reusing one with a key is catastrophic.
- **The key ID travels with the data**, so rotation is possible. Old keys must be kept as long as histories using them exist.

**Python detail:** a codec's `encode` and `decode` are `async`, because real ones often call a KMS over the network.

### Not run in this lab

mTLS needs certificates and a server configured for it, so I didn't run it. The code is short, and from the docs:

```python
client = await Client.connect(
    "your-namespace.a1b2c.tmprl.cloud:7233",
    namespace="your-namespace.a1b2c",
    tls=TLSConfig(client_cert=client_cert, client_private_key=client_private_key),
)
```

Also note that `shared/connection.py` reads settings from the environment, so credentials never appear in code.

## 5. Exercise

Write a security architecture for the fulfillment platform in `my-work/security.md`. Cover every boundary in the table, where each key and secret lives, who can read payloads, and what each environment shares.

## 6. Failure exercise

A developer's Workflow takes this input, and the ID is `order-jane.doe@example.com`. List everything that's wrong, with the fix for each:

```python
@dataclass
class Checkout:
    card_number: str
    cvv: str
    customer_email: str
```

Also: your encryption key is lost. What happens to running Workflows? To completed ones?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. How do you protect sensitive Workflow data?
2. What isn't covered by a Payload Codec?
3. Why does a Codec Server need strict access control?
4. What must you keep to rotate keys safely?
5. Why are separate namespaces and credentials per environment essential?

## 9. Gate

Type `/gate`. You'll explain each trust boundary in a design.

## 10. Next step

**Phase 15: Cloud and Self-Hosted.**
