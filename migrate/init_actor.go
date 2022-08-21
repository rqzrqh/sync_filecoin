package migrate

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	init_ "github.com/filecoin-project/lotus/chain/actors/builtin/init"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/rqzrqh/sync_filecoin/common"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func HandleInitActorChanges(ctx context.Context, node api.FullNode, curTs *types.TipSet, actList []common.ActorInfo, actorChanges []common.ActorInfo) (*common.ChainInitActor, error) {

	if len(actList) > 1 {
		return nil, xerrors.New("init actor count error")
	}

	if len(actList) == 0 {
		return nil, nil
	}

	sto := store.ActorStore(ctx, blockstore.NewAPIBlockstore(node))

	actInfo := actList[0]
	var out common.ChainInitActor

	addressToID := map[address.Address]address.Address{}

	if curTs.Height() == 1 {
		initActorState, err := init_.Load(sto, &actInfo.Act)
		if err != nil {
			return nil, err
		}

		for _, builtinAddress := range []address.Address{
			builtin.SystemActorAddr, builtin.InitActorAddr,
			builtin.RewardActorAddr, builtin.CronActorAddr, builtin.StoragePowerActorAddr, builtin.StorageMarketActorAddr,
			builtin.VerifiedRegistryActorAddr, builtin.BurntFundsActorAddr,
		} {
			addressToID[builtinAddress] = builtinAddress
		}

		if err := initActorState.ForEachActor(func(id abi.ActorID, addr address.Address) error {
			idAddr, err := address.NewIDAddress(uint64(id))
			if err != nil {
				return err
			}
			addressToID[addr] = idAddr

			return nil
		}); err != nil {
			return nil, err
		}

		out = common.ChainInitActor{
			AddressIDMap: addressToID,
		}

		return &out, nil
	}

	prevActor, err := node.StateGetActor(ctx, actInfo.Addr, curTs.Key())
	if err != nil {
		return nil, xerrors.Errorf("loading previous init actor: %w", err)
	}

	prevState, err := init_.Load(sto, prevActor)
	if err != nil {
		return nil, xerrors.Errorf("loading previous init actor state: %w", err)
	}

	curState, err := init_.Load(sto, &actInfo.Act)
	if err != nil {
		return nil, err
	}

	mtx := sync.Mutex{}

	grp, _ := errgroup.WithContext(ctx)

	for _, act := range actorChanges {

		a := act
		grp.Go(func() error {
			actorID1, _, err1 := prevState.ResolveAddress(a.Addr)
			if err1 != nil {
				return err
			}

			actorID2, _, err2 := curState.ResolveAddress(a.Addr)
			if err2 != nil {
				return err2
			}

			if err2 == nil {
				if actorID1 != actorID2 {
					mtx.Lock()
					addressToID[a.Addr] = actorID2
					mtx.Unlock()
				}
			} else {
				return err2
			}

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	out = common.ChainInitActor{
		AddressIDMap: addressToID,
	}
	return &out, nil
}
