package cloudevent

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const (
	// ERC721DIDMethod is the method for a ERC721 NFT DID.
	ERC721DIDMethod = "erc721"
	// EthrDIDMethod is the method for a Ethereum Address DID.
	EthrDIDMethod = "ethr"
	// ERC20DIDMethod is the method for a ERC20 token DID.
	ERC20DIDMethod = "erc20"
)

var errInvalidDID = errors.New("invalid DID")

// ERC721DID is a Decentralized Identifier for a ERC721 NFT.
type ERC721DID struct {
	ChainID         uint64         `json:"chainId"`
	ContractAddress common.Address `json:"contract"`
	TokenID         *big.Int       `json:"tokenId"`
}

// DecodeERC721DID decodes a DID string into a DID struct.
func DecodeERC721DID(did string) (ERC721DID, error) {
	// sample did "did:erc721:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1"
	parts := strings.Split(did, ":")
	if len(parts) != 5 {
		if len(parts) == 4 && parts[1] == "nft" {
			return DecodeLegacyNFTDID(did)
		}
		return ERC721DID{}, errInvalidDID
	}
	if parts[0] != "did" {
		return ERC721DID{}, fmt.Errorf("%w, incorrect DID prefix %s", errInvalidDID, parts[0])
	}
	if parts[1] != ERC721DIDMethod {
		return ERC721DID{}, fmt.Errorf("%w, incorrect DID method %s", errInvalidDID, parts[1])
	}
	chainID, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return ERC721DID{}, fmt.Errorf("%w, invalid chain ID %s", errInvalidDID, parts[2])
	}
	addrBytes := parts[3]
	if !common.IsHexAddress(addrBytes) {
		return ERC721DID{}, fmt.Errorf("%w, invalid contract address %s", errInvalidDID, addrBytes)
	}
	tokenID, ok := big.NewInt(0).SetString(parts[4], 10)
	if !ok {
		return ERC721DID{}, fmt.Errorf("%w, invalid token ID %s", errInvalidDID, parts[4])
	}
	if tokenID.Sign() < 0 {
		return ERC721DID{}, fmt.Errorf("%w, token ID cannot be negative %s", errInvalidDID, parts[4])
	}

	return ERC721DID{
		ChainID:         chainID,
		ContractAddress: common.HexToAddress(addrBytes),
		TokenID:         tokenID,
	}, nil
}

// String returns the string representation of the NFTDID.
func (e ERC721DID) String() string {
	return "did:" + ERC721DIDMethod + ":" + strconv.FormatUint(e.ChainID, 10) + ":" + e.ContractAddress.Hex() + ":" + e.TokenID.String()
}

// EthrDID is a Decentralized Identifier for an Ethereum contract.
type EthrDID struct {
	ChainID         uint64         `json:"chainId"`
	ContractAddress common.Address `json:"contract"`
}

// DecodeEthrDID decodes a Ethr DID string into a DID struct.
func DecodeEthrDID(did string) (EthrDID, error) {
	chainID, contractAddress, err := decodeAddressDID(did, EthrDIDMethod)
	if err != nil {
		return EthrDID{}, err
	}
	return EthrDID{
		ChainID:         chainID,
		ContractAddress: contractAddress,
	}, nil
}

// String returns the string representation of the EthrDID.
func (e EthrDID) String() string {
	return encodeAddressDID(EthrDIDMethod, e.ChainID, e.ContractAddress)
}

// ERC20DID is a Decentralized Identifier for an ERC20 token.
type ERC20DID struct {
	ChainID         uint64         `json:"chainId"`
	ContractAddress common.Address `json:"contract"`
}

// DecodeERC20DID decodes a ERC20 DID string into a DID struct.
func DecodeERC20DID(did string) (ERC20DID, error) {
	chainID, contractAddress, err := decodeAddressDID(did, ERC20DIDMethod)
	if err != nil {
		return ERC20DID{}, err
	}
	return ERC20DID{
		ChainID:         chainID,
		ContractAddress: contractAddress,
	}, nil
}

// String returns the string representation of the ERC20DID.
func (e ERC20DID) String() string {
	return encodeAddressDID(ERC20DIDMethod, e.ChainID, e.ContractAddress)
}

func decodeAddressDID(did string, method string) (uint64, common.Address, error) {
	// sample did "did:method:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"
	parts := strings.Split(did, ":")
	if len(parts) != 4 {
		return 0, common.Address{}, errInvalidDID
	}
	if parts[0] != "did" {
		return 0, common.Address{}, fmt.Errorf("%w, wrong DID prefix %s", errInvalidDID, parts[0])
	}
	if parts[1] != method {
		return 0, common.Address{}, fmt.Errorf("%w, wrong DID method %s", errInvalidDID, parts[1])
	}
	chainID, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, common.Address{}, fmt.Errorf("%w, invalid chain ID %s", errInvalidDID, parts[2])
	}
	addrBytes := parts[3]
	if !common.IsHexAddress(addrBytes) {
		return 0, common.Address{}, fmt.Errorf("%w, invalid contract address %s", errInvalidDID, addrBytes)
	}

	return chainID, common.HexToAddress(addrBytes), nil
}

func encodeAddressDID(method string, chainID uint64, contractAddress common.Address) string {
	return "did:" + method + ":" + strconv.FormatUint(chainID, 10) + ":" + contractAddress.Hex()
}

// DecodeLegacyNFTDID is a legacy decoder for NFT DIDs that use the format "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1"
// You most likely want to use DecodeERC721DID instead.
func DecodeLegacyNFTDID(did string) (ERC721DID, error) {
	// sample did "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1"
	parts := strings.Split(did, ":")
	if len(parts) != 4 {
		return ERC721DID{}, errInvalidDID
	}
	if parts[0] != "did" {
		return ERC721DID{}, fmt.Errorf("%w, incorrect DID prefix %s", errInvalidDID, parts[0])
	}
	if parts[1] != "nft" {
		return ERC721DID{}, fmt.Errorf("%w, incorrect DID method %s", errInvalidDID, parts[1])
	}
	nftParts := strings.Split(parts[3], "_")
	if len(nftParts) != 2 {
		return ERC721DID{}, fmt.Errorf("%w, incorrect NFT format %s", errInvalidDID, parts[3])
	}
	tokenID, ok := big.NewInt(0).SetString(nftParts[1], 10)
	if !ok {
		return ERC721DID{}, fmt.Errorf("%w, invalid token ID %s", errInvalidDID, nftParts[1])
	}
	if tokenID.Sign() < 0 {
		return ERC721DID{}, fmt.Errorf("%w, token ID cannot be negative %s", errInvalidDID, nftParts[1])
	}
	addrBytes := nftParts[0]
	if !common.IsHexAddress(addrBytes) {
		return ERC721DID{}, fmt.Errorf("%w, invalid contract address %s", errInvalidDID, addrBytes)
	}
	chainID, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return ERC721DID{}, fmt.Errorf("%w, invalid chain ID %s", errInvalidDID, parts[2])
	}

	return ERC721DID{
		ChainID:         chainID,
		ContractAddress: common.HexToAddress(addrBytes),
		TokenID:         tokenID,
	}, nil
}
