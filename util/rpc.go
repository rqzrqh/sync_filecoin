package util

import (
	"context"
	"net/http"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// copy from "github.com/filecoin-project/lotus/api/client" client.go
func NewFullNodeRPCV1(ctx context.Context, addr string, requestHeader http.Header) (api.FullNode, jsonrpc.ClientCloser, error) {
	var res v1api.FullNodeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "Filecoin",
		api.GetInternalStructs(&res), requestHeader)

	return &res, closer, err
}

func GetFullNodeAPIUsingCredentials(ctx context.Context, listenAddr, token string) (api.FullNode, jsonrpc.ClientCloser, error) {
	parsedAddr, err := ma.NewMultiaddr(listenAddr)
	if err != nil {
		return nil, nil, err
	}

	_, addr, err := manet.DialArgs(parsedAddr)
	if err != nil {
		return nil, nil, err
	}

	return NewFullNodeRPCV1(ctx, apiURI(addr), tokenHeaders(token))
}

func GetFullNodeAPIWithoutCredentials(ctx context.Context, listenAddr string) (api.FullNode, jsonrpc.ClientCloser, error) {
	parsedAddr, err := ma.NewMultiaddr(listenAddr)
	if err != nil {
		return nil, nil, err
	}

	_, addr, err := manet.DialArgs(parsedAddr)
	if err != nil {
		return nil, nil, err
	}

	return NewFullNodeRPCV1(ctx, apiURI(addr), emptyHeaders())
}

func apiURI(addr string) string {
	return "ws://" + addr + "/rpc/v1"
}

func tokenHeaders(token string) http.Header {
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	return headers
}

func emptyHeaders() http.Header {
	headers := http.Header{}
	return headers
}
