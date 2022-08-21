package migrate

import (
	"context"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/builtin/paych"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/rqzrqh/sync_filecoin/common"
)

func HandlePaymentChannelChanges(ctx context.Context, node api.FullNode, actList []common.ActorInfo) ([]*common.PaymentChannel, error) {

	sto := store.ActorStore(ctx, blockstore.NewAPIBlockstore(node))
	out := make([]*common.PaymentChannel, 0)

	for _, actInfo := range actList {

		pcState, err := paych.Load(sto, &actInfo.Act)
		if err != nil {
			log.Errorw("paych.Load", "err", err)
			return nil, err
		}
		pcInfo := common.PaymentChannel{
			LaneStates: make(map[uint64]common.LaneState),
		}
		pcInfo.From, _ = pcState.From()
		pcInfo.To, _ = pcState.To()
		pcInfo.SettlingAt, _ = pcState.SettlingAt()
		pcInfo.ToSend, _ = pcState.ToSend()
		pcInfo.LaneCount, _ = pcState.LaneCount()
		_ = pcState.ForEachLaneState(func(idx uint64, dl paych.LaneState) error {
			l := common.LaneState{}
			l.Nonce, _ = dl.Nonce()
			l.Redeemed, _ = dl.Redeemed()
			pcInfo.LaneStates[idx] = l
			return nil
		})
		out = append(out, &pcInfo)
	}

	return out, nil
}
