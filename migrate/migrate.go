package migrate

import (
	"context"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/rqzrqh/sync_filecoin/common"
)

var log = logging.Logger("migrate")

func GetFullTipSet(ctx context.Context, node api.FullNode, prevTsChangeAddressList []address.Address, syncPrevTsActorState bool, prevTs *types.TipSet, curTs *types.TipSet, curHeightKnownBlockMessages map[cid.Cid]*api.BlockMessages) (*common.FullTipSet, error) {
	height := curTs.Height()
	tsk := curTs.Key()

	start := time.Now()
	defer func() {
		log.Infow("migrate", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	// 1. collect raw tipset info
	prevTsAllChangedActors, curTsAllBlockMessages, curTsStateComputeOutout, curTsChangeAddressList, err := CollectRawFullTipSetInfo(ctx, node, prevTsChangeAddressList, syncPrevTsActorState, prevTs, curTs, curHeightKnownBlockMessages)
	if err != nil {
		return nil, err
	}

	// 2. process
	fts, err := Process(ctx, node, prevTs, curTs, prevTsAllChangedActors, curTsAllBlockMessages, curTsStateComputeOutout, curTsChangeAddressList)
	if err != nil {
		return nil, err
	}

	return fts, nil
}
