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

### Example CloudEvent JSON

When working with DIMO services, your CloudEvent payload should follow this format:

```json
{
  "id": "unique-event-identifier",
  "source": "0xEthereumAddress",
  "producer": "did:erc721:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
  "specversion": "1.0",
  "subject": "did:erc721:1:0x123456789abcdef0123456789abcdef012345678:42",
  "time": "2025-03-04T12:00:00Z",
  "type": "dimo.status",
  "datacontenttype": "application/json",
  "dataversion": "default/v1.0",
  "signature": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12",
  "data": {
    "signals": [
      {
        "name": "powertrainTransmissionTravelledDistance",
        "timestamp": "2025-03-04T12:00:00Z",
        "value": 12345.67
      },
      {
        "name": "speed",
        "timestamp": "2025-03-04T12:01:00Z",
        "value": 55
      },
      {
        "name": "powertrainType",
        "timestamp": "2025-03-04T12:03:00Z",
        "value": "COMBUSTION"
      }
    ],
    "vin": "1GGCM82633A123456"
  }
}
```

### CloudEvent Headers

Each CloudEvent contains the following header fields:

| Field           | Description                                                                                                               |
| --------------- | ------------------------------------------------------------------------------------------------------------------------- |
| ID              | A unique identifier for the event. The combination of ID and Source must be unique.                                       |
| Source          | Typically an Ethereum address. In many DIMO services                                                                      |
| Producer        | The DID of the entity that produced the payload. Ex. `did:erc721:<chainId>:<contractAddress>:<tokenId>`.                  |
| SpecVersion     | The version of CloudEvents specification used. This is always hardcoded as "1.0".                                         |
| Subject         | The DID which denotes the subject of the event. Ex. `did:erc721:<chainId>:<contractAddress>:<tokenId>`.                   |
| Time            | The time at which the event occurred. Format as RFC3339 timestamp.                                                        |
| Type            | Describes the type of event - must be one of the predefined DIMO types.                                                   |
| DataContentType | The MIME type for the data field. When using JSON (the most common case), this should be "application/json".              |
| DataSchema      | URI pointing to a schema for the data field.                                                                              |
| DataVersion     | An optional way for the data provider to specify the version of the data structure in the payload (e.g., "default/v1.0"). |
| Signature       | An optional cryptographic signature of the CloudEvent's data field for verification purposes.                             |
| Extras          | Additional custom fields.                                                                                                 |

The DIMO-specific extensions to the CloudEvents specification include:

- `Producer`: Provides additional context about the specific instance, process, or device that created the event
- `DataVersion`: A DIMO-specific extension that is unique to each source. This can be used by a source to determine the shape of the data field, enabling version-based data processing
- `Signature`: An optional cryptographic signature field for verifying the integrity and authenticity of the CloudEvent's data

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

### ERC721 DID

The ERC721 DID format is used to identify ERC721 tokens on a blockchain:

```
did:erc721:<chainID>:<contractAddress>:<tokenID>
```

Example: `did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:123`

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
        ID:             "unique-id",
        Source:         "0xConnectionLicenseAddress",
        Producer:       cloudevent.ERC721DID{
          ChainID: 1,
          ContractAddress: "0x123456789abcdef0123456789abcdef012345678",
          TokenID: big.NewInt(123),
        }.String(),
        Subject:         cloudevent.ERC721DID{
          ChainID: 1,
          ContractAddress: "0x123456789abcdef0123456789abcdef012345678",
          TokenID: big.NewInt(123),
        }.String(),
        Time:           time.Now().UTC(),
        Type:           cloudevent.TypeStatus,
        DataContentType: "application/json",
        DataVersion:    "default/v1.0",
        Signature:      "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12",
    },
    Data: MyDataType{
        // Your data fields
    },
}
```

### Working with DIDs

```go
// Parse an ERC721 DID
erc721DID, err := cloudevent.DecodeERC721DID("did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:123")
if err != nil {
    // Handle error
}

// Convert back to string
didString := erc721DID.String()
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
