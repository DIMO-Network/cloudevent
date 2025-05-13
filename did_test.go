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
		expectedDID   cloudevent.ERC721DID
		expectedError bool
	}{
		{
			name:  "valid DID",
			input: "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:123",
			expectedDID: cloudevent.ERC721DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(123),
			},
		},
		{
			name:  "valid legacy DID",
			input: "did:nft:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_123",
			expectedDID: cloudevent.ERC721DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(123),
			},
		},
		{
			name:          "invalid format - wrong part count",
			input:         "did:erc721:1",
			expectedDID:   cloudevent.ERC721DID{},
			expectedError: true,
		},
		{
			name:          "invalid format - wrong token part count",
			input:         "did:erc721:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
			expectedDID:   cloudevent.ERC721DID{},
			expectedError: true,
		},
		{
			name:          "invalid tokenID",
			input:         "did:erc721:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:notanumber",
			expectedDID:   cloudevent.ERC721DID{},
			expectedError: true,
		},
		{
			name:          "negative tokenID",
			input:         "did:erc721:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:-123",
			expectedDID:   cloudevent.ERC721DID{},
			expectedError: true,
		},
		{
			name:          "invalid DID string - wrong prefix",
			input:         "invalidprefix:erc721:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:1",
			expectedDID:   cloudevent.ERC721DID{},
			expectedError: true,
		},
		{
			name:          "invalid DID string - wrong method",
			input:         "did:invalid:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:1",
			expectedDID:   cloudevent.ERC721DID{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			did, err := cloudevent.DecodeERC721DID(tt.input)

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
			input:         "did:ethr:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:notanumber",
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

func TestERC721DID_String(t *testing.T) {
	tests := []struct {
		name     string
		did      cloudevent.ERC721DID
		expected string
	}{
		{
			name: "valid ERC721 DID",
			did: cloudevent.ERC721DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(123),
			},
			expected: "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:123",
		},
		{
			name: "ERC721 DID with zero token ID",
			did: cloudevent.ERC721DID{
				ChainID:         1,
				ContractAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
				TokenID:         big.NewInt(0),
			},
			expected: "did:erc721:1:0x1234567890123456789012345678901234567890:0",
		},
		{
			name: "ERC721 DID with zero value token ID",
			did: cloudevent.ERC721DID{
				ChainID:         1,
				ContractAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			},
			expected: "did:erc721:1:0x1234567890123456789012345678901234567890:<nil>",
		},
		{
			name: "ERC721 DID with large token ID",
			did: cloudevent.ERC721DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(1234567890123456789),
			},
			expected: "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:1234567890123456789",
		},
		{
			name:     "zero ERC721 DID",
			did:      cloudevent.ERC721DID{},
			expected: "did:erc721:0:0x0000000000000000000000000000000000000000:<nil>",
		},
		{
			name: "ERC721 DID with zero chain ID",
			did: cloudevent.ERC721DID{
				ChainID:         0,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
				TokenID:         big.NewInt(123),
			},
			expected: "did:erc721:0:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:123",
		},
		{
			name: "ERC721 DID with zero address",
			did: cloudevent.ERC721DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
				TokenID:         big.NewInt(123),
			},
			expected: "did:erc721:137:0x0000000000000000000000000000000000000000:123",
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

func TestDecodeERC20DID(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedDID   cloudevent.ERC20DID
		expectedError bool
	}{
		{
			name:  "valid DID",
			input: "did:erc20:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
			expectedDID: cloudevent.ERC20DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
			},
		},
		{
			name:          "invalid format - wrong part count",
			input:         "did:erc20:1",
			expectedDID:   cloudevent.ERC20DID{},
			expectedError: true,
		},
		{
			name:          "invalid contract address",
			input:         "did:erc20:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:extra",
			expectedDID:   cloudevent.ERC20DID{},
			expectedError: true,
		},
		{
			name:          "invalid DID string - wrong prefix",
			input:         "invalidprefix:erc20:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
			expectedDID:   cloudevent.ERC20DID{},
			expectedError: true,
		},
		{
			name:          "invalid DID string - wrong method",
			input:         "did:invalid:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
			expectedDID:   cloudevent.ERC20DID{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			did, err := cloudevent.DecodeERC20DID(tt.input)

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

func TestERC20DID_String(t *testing.T) {
	tests := []struct {
		name     string
		did      cloudevent.ERC20DID
		expected string
	}{
		{
			name: "valid ERC20 DID",
			did: cloudevent.ERC20DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
			},
			expected: "did:erc20:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
		},
		{
			name: "ERC20 DID on mainnet",
			did: cloudevent.ERC20DID{
				ChainID:         1,
				ContractAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			},
			expected: "did:erc20:1:0x1234567890123456789012345678901234567890",
		},
		{
			name: "ERC20 DID with zero address",
			did: cloudevent.ERC20DID{
				ChainID:         137,
				ContractAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			},
			expected: "did:erc20:137:0x0000000000000000000000000000000000000000",
		},
		{
			name:     "zero ERC20 DID",
			did:      cloudevent.ERC20DID{},
			expected: "did:erc20:0:0x0000000000000000000000000000000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.did.String()
			require.Equal(t, tt.expected, result)
		})
	}
}
