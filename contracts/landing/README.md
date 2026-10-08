# Landing Contracts

`raw_api_payload.schema.yaml` documents the immutable raw envelope written around every API response.

The envelope exists for replay, auditing, lineage, and debugging. It includes source/endpoint identity, run date, masked request metadata, response status, raw payload, and ingestion timing.

Landing contracts describe the envelope, not endpoint-specific typed tables. Endpoint payload mapping belongs in Staging contracts. HTTP error envelopes may exist in Landing; Staging only loads successful response payloads.
