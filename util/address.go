package util

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// GenerateConsensusAddressFromPubkey는 base64로 인코딩된 consensus pubkey에서
// consensus address를 생성합니다 (SHA256 해시의 앞 20바이트를 HEX로 변환)
func GenerateConsensusAddressFromPubkey(pubKeyBase64 string) string {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKeyBase64)
	if err != nil {
		return ""
	}
	
	sha256Hash := sha256.Sum256(pubKeyBytes)
	consensusAddr20 := sha256Hash[:20]
	
	return strings.ToUpper(hex.EncodeToString(consensusAddr20))
}
