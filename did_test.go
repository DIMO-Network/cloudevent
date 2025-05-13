package cloudevent_test

import (
	"math/big"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestDecodeDID(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedDID   cloudevent.NFTDID
		expectedError bool
	}{
		{
			name:  "valid DID",
			input: "did:nft:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_123",
			expectedDID: cloudevent.NFTDID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(123),
			},
		},
		{
			name:          "invalid format - wrong part count",
			input:         "did:nft:1",
			expectedDID:   cloudevent.NFTDID{},
			expectedError: true,
		},
		{
			name:          "invalid format - wrong token part count",
			input:         "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
			expectedDID:   cloudevent.NFTDID{},
			expectedError: true,
		},
		{
			name:          "invalid tokenID",
			input:         "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_notanumber",
			expectedDID:   cloudevent.NFTDID{},
			expectedError: true,
		},
		{
			name:          "invalid DID string - wrong prefix",
			input:         "invalidprefix:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
			expectedDID:   cloudevent.NFTDID{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			did, err := cloudevent.DecodeNFTDID(tt.input)

			// Check if the error matches the expected error
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Check if the DID struct matches the expected DID
			require.Equal(t, tt.expectedDID, did)
		})
	}
}

func TestDecodeEthrDID(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedDID   cloudevent.EthrDID
		expectedError bool
	}{
		{
			name:  "valid DID",
			input: "did:ethr:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
			expectedDID: cloudevent.EthrDID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
			},
		},
		{
			name:          "invalid format - wrong part count",
			input:         "did:ethr:1",
			expectedDID:   cloudevent.EthrDID{},
			expectedError: true,
		},
		{
			name:          "invalid contract address",
			input:         "did:ethr:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_notanumber",
			expectedDID:   cloudevent.EthrDID{},
			expectedError: true,
		},
		{
			name:          "invalid DID string - wrong prefix",
			input:         "invalidprefix:ethr:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
			expectedDID:   cloudevent.EthrDID{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			did, err := cloudevent.DecodeEthrDID(tt.input)

			// Check if the error matches the expected error
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Check if the DID struct matches the expected DID
			require.Equal(t, tt.expectedDID, did)
		})
	}
}

func TestNFTDID_String(t *testing.T) {
	tests := []struct {
		name     string
		did      cloudevent.NFTDID
		expected string
	}{
		{
			name: "valid NFT DID",
			did: cloudevent.NFTDID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(123),
			},
			expected: "did:nft:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_123",
		},
		{
			name: "NFT DID with zero token ID",
			did: cloudevent.NFTDID{
				ChainID:         1,
				ContractAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
				TokenID:         big.NewInt(0),
			},
			expected: "did:nft:1:0x1234567890123456789012345678901234567890_0",
		},
		{
			name: "NFT DID with zero value token ID",
			did: cloudevent.NFTDID{
				ChainID:         1,
				ContractAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			},
			expected: "did:nft:1:0x1234567890123456789012345678901234567890_<nil>",
		},
		{
			name: "NFT DID with large token ID",
			did: cloudevent.NFTDID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(1234567890123456789),
			},
			expected: "did:nft:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1234567890123456789",
		},
		{
			name:     "zero NFT DID",
			did:      cloudevent.NFTDID{},
			expected: "did:nft:0:0x0000000000000000000000000000000000000000_<nil>",
		},
		{
			name: "NFT DID with zero chain ID",
			did: cloudevent.NFTDID{
				ChainID:         0,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(123),
			},
			expected: "did:nft:0:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_123",
		},
		{
			name: "NFT DID with zero address",
			did: cloudevent.NFTDID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
				TokenID:         big.NewInt(123),
			},
			expected: "did:nft:137:0x0000000000000000000000000000000000000000_123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.did.String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestEthrDID_String(t *testing.T) {
	tests := []struct {
		name     string
		did      cloudevent.EthrDID
		expected string
	}{
		{
			name: "valid Ethr DID",
			did: cloudevent.EthrDID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
			},
			expected: "did:ethr:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
		},
		{
			name: "Ethr DID on mainnet",
			did: cloudevent.EthrDID{
				ChainID:         1,
				ContractAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			},
			expected: "did:ethr:1:0x1234567890123456789012345678901234567890",
		},
		{
			name: "Ethr DID with zero address",
			did: cloudevent.EthrDID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			},
			expected: "did:ethr:137:0x0000000000000000000000000000000000000000",
		},
		{
			name:     "zero Ethr DID",
			did:      cloudevent.EthrDID{},
			expected: "did:ethr:0:0x0000000000000000000000000000000000000000",
		},
		{
			name: "Ethr DID with zero chain ID",
			did: cloudevent.EthrDID{
				ChainID:         0,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
			},
			expected: "did:ethr:0:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.did.String()
			require.Equal(t, tt.expected, result)
		})
	}
}
