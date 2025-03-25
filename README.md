# DIMO CloudEvent

![GitHub license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
[![GoDoc](https://godoc.org/github.com/DIMO-Network/cloudevent?status.svg)](https://godoc.org/github.com/DIMO-Network/cloudevent)
[![Go Report Card](https://goreportcard.com/badge/github.com/DIMO-Network/cloudevent)](https://goreportcard.com/report/github.com/DIMO-Network/cloudevent)

A Go library for working with CloudEvents in the DIMO ecosystem, with specialized support for Decentralized Identifiers (DIDs).

## Overview

DIMO CloudEvent is designed for developers building applications within the DIMO ecosystem who need to:

- Work with standardized event data following the CloudEvents specification
- Process events with blockchain-based identifiers (DIDs)
- Build event-driven applications with persistent, well-structured event data

This library implements a DIMO-specific profile of the [CloudEvents specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md), extending the standard with additional features for decentralized identifiers and blockchain data structures while maintaining compatibility with the core specification.

## CloudEvent Format

### CloudEvents Specification

DIMO CloudEvents are a derived standard from the official CloudEvents specification v1.0. While maintaining full compatibility with the base standard, DIMO CloudEvents include additional fields and conventions specific to the DIMO ecosystem.

The core structure is:

```go
type CloudEvent[A any] struct {
    CloudEventHeader
    Data A `json:"data"`
}
```

### CloudEvent Headers

Each CloudEvent contains the following header fields:

| Field           | Description                                                 |
| --------------- | ----------------------------------------------------------- |
| ID              | Unique identifier for the event                             |
| Source          | Context in which the event happened                         |
| Producer        | Specific instance/process that created the event            |
| SpecVersion     | CloudEvents spec version (always "1.0")                     |
| Subject         | Subject of the event within the producer's context          |
| Time            | Time at which the event occurred                            |
| Type            | Type of event (e.g., "dimo.status")                         |
| DataContentType | MIME type for the data field (typically "application/json") |
| DataSchema      | URI pointing to a schema for the data field                 |
| DataVersion     | Version of the data type                                    |
| Extras          | Additional custom fields                                    |

The DIMO-specific extensions to the CloudEvents specification include:

- `Producer`: Provides additional context about the specific instance, process, or device that created the event
- `DataVersion`: A DIMO-specific extension that is unique to each source. This can be used by a source to determine the shape of the data field, enabling version-based data processing

### Event Uniqueness

A CloudEvent is uniquely identified by the combination of the following fields:

- `Subject`
- `Time`
- `Type`
- `Source`
- `ID`

This combination forms the "index key" for the event and is used for deduplication and retrieval purposes.

### DIMO Event Types

The library defines several DIMO-specific event types:

- `dimo.status`: Used for status updates
- `dimo.fingerprint`: Used for fingerprint updates
- `dimo.verifiablecredential`: Used for verifiable credentials
- `dimo.unknown`: Used for unknown events

DIMO services expect the `Type` field to be one of these predefined types. Using custom or undefined types may result in events being improperly processed or rejected by DIMO services.

## Decentralized Identifier (DID) Formats

The library provides support for two types of DIDs:

### NFT DID

The NFT DID format is used to identify NFTs on a blockchain:

```
did:nft:<chainID>:<contractAddress>_<tokenID>
```

Example: `did:nft:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_123`

- `chainID`: The blockchain network ID (e.g., 1 for Ethereum mainnet, 137 for Polygon)
- `contractAddress`: Ethereum hex address of the NFT contract
- `tokenID`: The numeric token ID

### Ethereum DID

The Ethereum DID format is used to identify Ethereum accounts or contracts:

```
did:ethr:<chainID>:<address>
```

Example: `did:ethr:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF`

- `chainID`: The blockchain network ID
- `address`: Ethereum hex address

## Usage Examples

### Creating a CloudEvent

```go
event := cloudevent.CloudEvent[MyDataType]{
    CloudEventHeader: cloudevent.CloudEventHeader{
        ID:       "unique-id",
        Source:   "my-service",
        Producer: "instance-1",
        Subject:  "device-123",
        Time:     time.Now().UTC(),
        Type:     cloudevent.TypeStatus,
        DataVersion: "1.0",  // Version of your data structure
    },
    Data: MyDataType{
        // Your data fields
    },
}
```

### Working with DIDs

```go
// Parse an NFT DID
nftDID, err := cloudevent.DecodeNFTDID("did:nft:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_123")
if err != nil {
    // Handle error
}

// Convert back to string
didString := nftDID.String()
```

## Development

Use `make` to manage the project building, testing, and linting.

```
> make help

Specify a subcommand:

  build                Build the code
  clean                Clean the project binaries
  tidy                 tidy the go modules
  test                 Run the all tests
  lint                 Run the linter
  format               Run the linter with fix
  migration            Generate migration file specify name with name=your_migration_name
  tools                Install all tools
  tools-golangci-lint  Install golangci-lint
  tools-migration      Install migration tool
```

## License

[Apache 2.0](LICENSE)
