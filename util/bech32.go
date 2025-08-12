package util

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/btcsuite/btcutil/bech32"
)

var (
	accountPrefix   string
	validatorPrefix string
	consensusPrefix string
)

func InitBech32Prefix(prefix Bech32Prefix) {
	accountPrefix = prefix.Account
	validatorPrefix = prefix.Validator
	consensusPrefix = prefix.Consensus
}

type Bech32Prefix struct {
	Account   string
	Validator string
	Consensus string
}

func ConvertAddress(address, fromPrefix, toPrefix string) (string, error) {
	if fromPrefix == toPrefix {
		return address, nil
	}

	hrp, data, err := bech32.Decode(address)
	if err != nil {
		return "", fmt.Errorf("failed to decode bech32 address: %w", err)
	}

	if hrp != fromPrefix {
		return "", fmt.Errorf("address prefix mismatch: expected %s, got %s", fromPrefix, hrp)
	}

	newAddress, err := bech32.Encode(toPrefix, data)
	if err != nil {
		return "", fmt.Errorf("failed to encode bech32 address: %w", err)
	}

	return newAddress, nil
}

func GetConsensusHexFromPubKeyString(pubKeyStr string) (string, error) {
	if !strings.HasPrefix(pubKeyStr, "{\"@type\":\"") {
		return "", fmt.Errorf("invalid pubkey format")
	}

	start := strings.Index(pubKeyStr, "\"key\":\"")
	if start == -1 {
		return "", fmt.Errorf("key field not found")
	}
	start += 7

	end := strings.Index(pubKeyStr[start:], "\"")
	if end == -1 {
		return "", fmt.Errorf("key field end not found")
	}

	keyStr := pubKeyStr[start : start+end]

	decoded, err := hex.DecodeString(keyStr)
	if err != nil {
		return "", fmt.Errorf("failed to decode hex key: %w", err)
	}

	consensusAddr, err := bech32.Encode(consensusPrefix, decoded)
	if err != nil {
		return "", fmt.Errorf("failed to encode consensus address: %w", err)
	}

	return consensusAddr, nil
}

func ConvertTokenAmount(amount string, fromDecimals, toDecimals int) string {
	if fromDecimals == toDecimals {
		return amount
	}

	amountFloat, err := strconv.ParseFloat(amount, 64)
	if err != nil {
		return amount
	}

	if fromDecimals > toDecimals {
		amountFloat = amountFloat / float64(10^(fromDecimals-toDecimals))
	} else {
		amountFloat = amountFloat * float64(10^(toDecimals-fromDecimals))
	}

	return fmt.Sprintf("%.6f", amountFloat)
}
