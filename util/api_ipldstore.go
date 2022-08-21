package util

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/lotus/api"
)

// copy from lotus 1.11.0 or 1.4.0  lotus-chainwatch

type APIIpldStore struct {
	ctx context.Context
	api api.FullNode
}

func NewAPIIpldStore(ctx context.Context, api api.FullNode) *APIIpldStore {
	return &APIIpldStore{
		ctx: ctx,
		api: api,
	}
}

func (ht *APIIpldStore) Context() context.Context {
	return ht.ctx
}

func (ht *APIIpldStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	raw, err := ht.api.ChainReadObj(ctx, c)
	if err != nil {
		return err
	}

	cu, ok := out.(cbg.CBORUnmarshaler)
	if ok {
		if err := cu.UnmarshalCBOR(bytes.NewReader(raw)); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("object does not implement CBORUnmarshaler: %T", out)
}

func (ht *APIIpldStore) Put(context.Context, interface{}) (cid.Cid, error) {
	return cid.Undef, fmt.Errorf("put is not implemented on APIIpldStore")
}
