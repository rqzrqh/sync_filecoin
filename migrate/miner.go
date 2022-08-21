package migrate

import (
	"context"
	"strings"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/actors/builtin/power"
	"github.com/filecoin-project/lotus/chain/events/state"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/util"
)

type SectorLifecycleEvent string

const (
	PreCommitAdded   = "PRECOMMIT_ADDED"
	PreCommitExpired = "PRECOMMIT_EXPIRED"

	CommitCapacityAdded = "COMMIT_CAPACITY_ADDED"

	SectorAdded      = "SECTOR_ADDED"
	SectorExpired    = "SECTOR_EXPIRED"
	SectorExtended   = "SECTOR_EXTENDED"
	SectorFaulted    = "SECTOR_FAULTED"
	SectorRecovering = "SECTOR_RECOVERING"
	SectorRecovered  = "SECTOR_RECOVERED"
	SectorTerminated = "SECTOR_TERMINATED"
)

type MinerSectorsEvent struct {
	MinerID   address.Address
	SectorIDs []uint64
	Event     SectorLifecycleEvent
}

type SectorDealEvent struct {
	MinerID  address.Address
	SectorID uint64
	DealIDs  []abi.DealID
}

type PartitionStatus struct {
	Terminated bitfield.BitField
	Expired    bitfield.BitField
	Faulted    bitfield.BitField
	InRecovery bitfield.BitField
	Recovered  bitfield.BitField
}

func HandleStorageMinerChanges(ctx context.Context, node api.FullNode, prevTs *types.TipSet, curTs *types.TipSet, powerActor *types.Actor, actList []common.ActorInfo) (
	[]*common.MinerPower, []*common.Miner, []*common.SectorInfo, []*common.SectorPrecommitInfo, []*common.MinerSectorEvent, []*common.SectorDealEvent, error) {

	ptsk := curTs.Parents()
	tsk := curTs.Key()

	grp1, _ := errgroup.WithContext(ctx)

	var minerPowers []*common.MinerPower
	var minerStates []miner.State
	var miners []*common.Miner

	grp1.Go(func() error {
		powers, states, err := getMinerPowerAndState(ctx, node, powerActor, actList)
		if err != nil {
			return err
		}

		minerPowers = powers
		minerStates = states
		return nil
	})

	grp1.Go(func() error {
		minerList, err := getMiners(ctx, node, ptsk, tsk, actList)
		if err != nil {
			return err
		}

		miners = minerList
		return nil
	})

	if err := grp1.Wait(); err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	var sectorInfos []*common.SectorInfo
	var sectorPrecommitInfos []*common.SectorPrecommitInfo
	var minerSectorEvents []*common.MinerSectorEvent
	var sectorDealEvents []*common.SectorDealEvent

	// TODO below is too slow
	return minerPowers, miners, sectorInfos, sectorPrecommitInfos, minerSectorEvents, sectorDealEvents, nil

	// 8 is arbitrary, idk what a good value here is.
	preCommitEvents := make(chan *MinerSectorsEvent, 8)
	sectorEvents := make(chan *MinerSectorsEvent, 8)
	partitionEvents := make(chan *MinerSectorsEvent, 8)
	dealEvents := make(chan *SectorDealEvent, 8)

	grp2, _ := errgroup.WithContext(ctx)

	// channel consumer
	grp2.Go(func() error {
		events, err := getPreCommitDealInfo(dealEvents)
		if err != nil {
			return err
		}

		sectorDealEvents = events
		return nil
	})

	grp2.Go(func() error {
		events, err := getMinerSectorEvents(ctx, sectorEvents, preCommitEvents, partitionEvents)
		if err != nil {
			return err
		}

		minerSectorEvents = events
		return nil
	})

	// channel producer
	grp2.Go(func() error {
		defer func() {
			close(preCommitEvents)
			close(dealEvents)
		}()
		precommitInfos, err := getMinerPreCommitInfo(ctx, node, ptsk, tsk, actList, minerStates, preCommitEvents, dealEvents)
		if err != nil {
			return err
		}

		sectorPrecommitInfos = precommitInfos
		return nil
	})

	grp2.Go(func() error {
		defer close(sectorEvents)
		infos, err := getMinerSectorInfo(ctx, node, ptsk, tsk, actList, sectorEvents)
		if err != nil {
			return err
		}

		sectorInfos = infos
		return nil
	})

	grp2.Go(func() error {
		defer close(partitionEvents)
		return getMinerPartitionsDifferences(ctx, node, tsk, actList, minerStates, partitionEvents)
	})

	if err := grp2.Wait(); err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	return minerPowers, miners, sectorInfos, sectorPrecommitInfos, minerSectorEvents, sectorDealEvents, nil
}

func getMinerPowerAndState(ctx context.Context, node api.FullNode, powerActor *types.Actor, actList []common.ActorInfo) ([]*common.MinerPower, []miner.State, error) {
	minerPowers := make([]*common.MinerPower, len(actList))
	minerStates := make([]miner.State, len(actList))
	mtx := sync.Mutex{}

	powerState, err := power.Load(util.NewAPIIpldStore(ctx, node), powerActor)
	if err != nil {
		return nil, nil, err
	}

	sto := store.ActorStore(ctx, blockstore.NewAPIBlockstore(node))

	grp, _ := errgroup.WithContext(ctx)

	for i, info := range actList {

		idx := i
		actInfo := info

		grp.Go(func() error {
			// Get miner raw and quality power
			var mi common.MinerPower
			mi.Act = actInfo

			// get miner claim from power actors claim map and store if found, else the miner had no claim at
			// this tipset
			claim, found, err := powerState.MinerPower(actInfo.Addr)
			if err != nil {
				return err
			}
			if found {
				mi.QalPower = claim.QualityAdjPower
				mi.RawPower = claim.RawBytePower
			}

			// Get the miner state
			mas, err := miner.Load(sto, &actInfo.Act)
			if err != nil {
				//log.Warnw("failed to find miner actor state", "address", act.Addr, "error", err)
				return err
			}

			mtx.Lock()
			minerStates[idx] = mas
			minerPowers[idx] = &mi
			mtx.Unlock()

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, nil, err
	}

	return minerPowers, minerStates, nil
}

func getMiners(ctx context.Context, node api.FullNode, ptsk types.TipSetKey, tsk types.TipSetKey, actList []common.ActorInfo) ([]*common.Miner, error) {
	out := make([]*common.Miner, len(actList))
	mtx := sync.Mutex{}

	grp, _ := errgroup.WithContext(ctx)

	for i, info := range actList {

		idx := i
		act := info

		grp.Go(func() error {

			mi, err := node.StateMinerInfo(ctx, act.Addr, tsk)
			if err != nil {
				if strings.Contains(err.Error(), types.ErrActorNotFound.Error()) {
					// TODO return nil or err?
					return nil
				} else {
					return err
				}
			}

			m := &common.Miner{
				MinerID:    act.Addr,
				OwnerAddr:  mi.Owner,
				WorkerAddr: mi.Worker,
				PeerID:     mi.PeerId,
				SectorSize: mi.SectorSize,
			}

			mtx.Lock()
			out[idx] = m
			mtx.Unlock()

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	return out, nil
}

func getMinerSectorInfo(ctx context.Context, node api.FullNode, ptsKey types.TipSetKey, tsKey types.TipSetKey, miners []common.ActorInfo, events chan<- *MinerSectorsEvent) ([]*common.SectorInfo, error) {

	sectorInfos := make([]*common.SectorInfo, 0)
	var addLock sync.Mutex

	grp, _ := errgroup.WithContext(ctx)
	for _, m := range miners {
		m := m
		grp.Go(func() error {
			changes, err := getMinerSectorChanges(ctx, node, ptsKey, tsKey, m.Addr)
			if err != nil {
				if strings.Contains(err.Error(), types.ErrActorNotFound.Error()) {
					return nil
				}
				return err
			}
			if changes == nil {
				return nil
			}
			var sectorsAdded []uint64
			var ccAdded []uint64
			var extended []uint64
			for _, added := range changes.Added {

				sectorInfo := common.SectorInfo{
					MinerID:               m.Addr,
					SectorID:              added.SectorNumber,
					SealedCid:             added.SealedCID,
					ActivationEpoch:       added.Activation,
					ExpirationEpoch:       added.Expiration,
					DealWeight:            added.DealWeight,
					VerifiedDealWeight:    added.VerifiedDealWeight,
					InitialPledge:         added.InitialPledge,
					ExpectedDayReward:     added.ExpectedDayReward,
					ExpectedStoragePledge: added.ExpectedStoragePledge,
				}

				addLock.Lock()
				sectorInfos = append(sectorInfos, &sectorInfo)
				addLock.Unlock()

				if len(added.DealIDs) == 0 {
					ccAdded = append(ccAdded, uint64(added.SectorNumber))
				} else {
					sectorsAdded = append(sectorsAdded, uint64(added.SectorNumber))
				}
			}

			for _, mod := range changes.Extended {
				extended = append(extended, uint64(mod.To.SectorNumber))
			}

			events <- &MinerSectorsEvent{
				MinerID:   m.Addr,
				SectorIDs: ccAdded,
				Event:     CommitCapacityAdded,
			}
			events <- &MinerSectorsEvent{
				MinerID:   m.Addr,
				SectorIDs: sectorsAdded,
				Event:     SectorAdded,
			}
			events <- &MinerSectorsEvent{
				MinerID:   m.Addr,
				SectorIDs: extended,
				Event:     SectorExtended,
			}
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	return sectorInfos, nil
}

func getPreCommitDealInfo(dealEvents <-chan *SectorDealEvent) ([]*common.SectorDealEvent, error) {
	sectorDealEvents := make([]*common.SectorDealEvent, 0)

	for sde := range dealEvents {
		for _, did := range sde.DealIDs {
			dealEvent := common.SectorDealEvent{
				DealID:   did,
				MinerID:  sde.MinerID,
				SectorID: sde.SectorID,
			}
			sectorDealEvents = append(sectorDealEvents, &dealEvent)
		}
	}

	return sectorDealEvents, nil
}

func getMinerPreCommitInfo(ctx context.Context, node api.FullNode, ptsKey types.TipSetKey, tsKey types.TipSetKey, miners []common.ActorInfo, minerStates []miner.State, sectorEvents chan<- *MinerSectorsEvent, sectorDeals chan<- *SectorDealEvent) ([]*common.SectorPrecommitInfo, error) {

	sectorPrecommitInfos := make([]*common.SectorPrecommitInfo, 0)

	grp, _ := errgroup.WithContext(ctx)
	for idx, m := range miners {
		m := m
		minerState := minerStates[idx]
		grp.Go(func() error {
			changes, err := getMinerPreCommitChanges(ctx, node, ptsKey, tsKey, m.Addr)
			if err != nil {
				if strings.Contains(err.Error(), types.ErrActorNotFound.Error()) {
					return nil
				}
				return err
			}
			if changes == nil {
				return nil
			}

			preCommitAdded := make([]uint64, len(changes.Added))
			for i, added := range changes.Added {
				if len(added.Info.DealIDs) > 0 {
					sectorDeals <- &SectorDealEvent{
						MinerID:  m.Addr,
						SectorID: uint64(added.Info.SectorNumber),
						DealIDs:  added.Info.DealIDs,
					}
				}

				var precommitInfo common.SectorPrecommitInfo

				if added.Info.ReplaceCapacity {
					precommitInfo.MinerID = m.Addr
					precommitInfo.SectorID = added.Info.SectorNumber
					precommitInfo.SealedCid = added.Info.SealedCID
					precommitInfo.SealRandEpoch = added.Info.SealRandEpoch
					precommitInfo.ExpirationEpoch = added.Info.Expiration
					precommitInfo.PrecommitDeposit = added.PreCommitDeposit
					precommitInfo.PrecommitEpoch = added.PreCommitEpoch
					precommitInfo.DealWeight = added.DealWeight
					precommitInfo.VerifiedDealWeight = added.VerifiedDealWeight
					precommitInfo.IsReplaceCapacity = added.Info.ReplaceCapacity
					precommitInfo.ReplaceSectorDeadline = added.Info.ReplaceSectorDeadline
					precommitInfo.ReplaceSectorPartition = added.Info.ReplaceSectorPartition
					precommitInfo.ReplaceSectorNumber = added.Info.ReplaceSectorNumber
				} else {
					precommitInfo.MinerID = m.Addr
					precommitInfo.SectorID = added.Info.SectorNumber
					precommitInfo.SealedCid = added.Info.SealedCID
					precommitInfo.SealRandEpoch = added.Info.SealRandEpoch
					precommitInfo.ExpirationEpoch = added.Info.Expiration
					precommitInfo.PrecommitDeposit = added.PreCommitDeposit
					precommitInfo.PrecommitEpoch = added.PreCommitEpoch
					precommitInfo.DealWeight = added.DealWeight
					precommitInfo.VerifiedDealWeight = added.VerifiedDealWeight
					precommitInfo.IsReplaceCapacity = added.Info.ReplaceCapacity
					//nil, // replace deadline
					//nil, // replace partition
					//nil, // replace sector
				}
				preCommitAdded[i] = uint64(added.Info.SectorNumber)
			}
			if len(preCommitAdded) > 0 {
				sectorEvents <- &MinerSectorsEvent{
					MinerID:   m.Addr,
					SectorIDs: preCommitAdded,
					Event:     PreCommitAdded,
				}
			}
			var preCommitExpired []uint64
			for _, removed := range changes.Removed {
				// TODO: we can optimize this to not load the AMT every time, if necessary.
				si, err := minerState.GetSector(removed.Info.SectorNumber)
				if err != nil {
					return err
				}
				if si == nil {
					preCommitExpired = append(preCommitExpired, uint64(removed.Info.SectorNumber))
				}
			}
			if len(preCommitExpired) > 0 {
				sectorEvents <- &MinerSectorsEvent{
					MinerID:   m.Addr,
					SectorIDs: preCommitExpired,
					Event:     PreCommitExpired,
				}
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	return sectorPrecommitInfos, nil
}

func getMinerSectorEvents(ctx context.Context, sectorEvents, preCommitEvents, partitionEvents <-chan *MinerSectorsEvent) ([]*common.MinerSectorEvent, error) {

	sectorsEvents := make([]*common.MinerSectorEvent, 0)
	var addLock sync.Mutex

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		innerGrp, _ := errgroup.WithContext(ctx)
		for mse := range sectorEvents {
			mse := mse
			innerGrp.Go(func() error {
				for _, sid := range mse.SectorIDs {
					sectorEvent := common.MinerSectorEvent{
						MinerID:  mse.MinerID,
						SectorID: sid,
						Event:    common.SectorLifecycleEvent(mse.Event),
					}

					addLock.Lock()
					sectorsEvents = append(sectorsEvents, &sectorEvent)
					addLock.Unlock()
				}
				return nil
			})
		}
		return innerGrp.Wait()
	})

	grp.Go(func() error {
		innerGrp, _ := errgroup.WithContext(ctx)
		for mse := range preCommitEvents {
			mse := mse
			innerGrp.Go(func() error {
				for _, sid := range mse.SectorIDs {
					sectorEvent := common.MinerSectorEvent{
						MinerID:  mse.MinerID,
						SectorID: sid,
						Event:    common.SectorLifecycleEvent(mse.Event),
					}

					addLock.Lock()
					sectorsEvents = append(sectorsEvents, &sectorEvent)
					addLock.Unlock()
				}
				return nil
			})
		}
		return innerGrp.Wait()
	})

	grp.Go(func() error {
		innerGrp, _ := errgroup.WithContext(ctx)
		for mse := range partitionEvents {
			mse := mse
			grp.Go(func() error {
				for _, sid := range mse.SectorIDs {
					sectorEvent := common.MinerSectorEvent{
						MinerID:  mse.MinerID,
						SectorID: sid,
						Event:    common.SectorLifecycleEvent(mse.Event),
					}

					addLock.Lock()
					sectorsEvents = append(sectorsEvents, &sectorEvent)
					addLock.Unlock()
				}
				return nil
			})
		}
		return innerGrp.Wait()
	})

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	return sectorsEvents, nil
}

func getMinerPartitionsDifferences(ctx context.Context, node api.FullNode, ptsKey types.TipSetKey, miners []common.ActorInfo, minerStates []miner.State, events chan<- *MinerSectorsEvent) error {
	grp, ctx := errgroup.WithContext(ctx)
	for idx, m := range miners {
		m := m
		minerState := &minerStates[idx]
		grp.Go(func() error {
			if err := diffMinerPartitions(ctx, node, ptsKey, m, minerState, events); err != nil {
				if strings.Contains(err.Error(), types.ErrActorNotFound.Error()) {
					return nil
				}
				return err
			}
			return nil
		})
	}
	return grp.Wait()
}

func getMinerSectorChanges(ctx context.Context, node api.FullNode, ptsk types.TipSetKey, tsk types.TipSetKey, addr address.Address) (*miner.SectorChanges, error) {
	pred := state.NewStatePredicates(node)
	changed, val, err := pred.OnMinerActorChange(addr, pred.OnMinerSectorChange())(ctx, ptsk, tsk)
	if err != nil {
		return nil, xerrors.Errorf("Failed to diff miner sectors amt: %w", err)
	}
	if !changed {
		return nil, nil
	}
	out := val.(*miner.SectorChanges)
	return out, nil
}

func getMinerPreCommitChanges(ctx context.Context, node api.FullNode, ptsk types.TipSetKey, tsk types.TipSetKey, addr address.Address) (*miner.PreCommitChanges, error) {
	pred := state.NewStatePredicates(node)
	changed, val, err := pred.OnMinerActorChange(addr, pred.OnMinerPreCommitChange())(ctx, ptsk, tsk)
	if err != nil {
		return nil, xerrors.Errorf("Failed to diff miner precommit amt: %w", err)
	}
	if !changed {
		return nil, nil
	}
	out := val.(*miner.PreCommitChanges)
	return out, nil
}

func diffMinerPartitions(ctx context.Context, node api.FullNode, ptsk types.TipSetKey, m common.ActorInfo, minerState *miner.State, events chan<- *MinerSectorsEvent) error {
	prevMiner, err := getMinerStateAt(ctx, node, m.Addr, ptsk)
	if err != nil {
		return err
	}
	curMiner := minerState
	dc, err := prevMiner.DeadlinesChanged(*curMiner)
	if err != nil {
		return err
	}
	if !dc {
		return nil
	}

	return nil
	panic("TODO")
}

func getMinerStateAt(ctx context.Context, node api.FullNode, maddr address.Address, tskey types.TipSetKey) (miner.State, error) {
	prevActor, err := node.StateGetActor(ctx, maddr, tskey)
	if err != nil {
		return nil, err
	}
	return miner.Load(store.ActorStore(ctx, blockstore.NewAPIBlockstore(node)), prevActor)
}
