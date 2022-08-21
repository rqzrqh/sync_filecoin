package util

import (
	"math/rand"
	"strings"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
)

// better way?
func IsWebsocketClosed(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "websocket") && strings.Contains(errStr, "closed")
}

const (
	MainNetStart = 1598306400
)

func EpochToTimestamp(epoch abi.ChainEpoch) int64 {
	return MainNetStart + int64(epoch)*int64(build.BlockDelaySecs)
}

func RandHexStr(length int) string {
	b := make([]byte, length)

	for i := range b {
		hexStr := "0123456789abcdef"
		b[i] = hexStr[rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(hexStr))]
	}

	return string(b)

}
