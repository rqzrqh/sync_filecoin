package migrate

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/util"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/power"
	"github.com/filecoin-project/lotus/chain/types"
)

func HandleStoragePowerChange(ctx context.Context, node api.FullNode, curTs *types.TipSet, actList []common.ActorInfo) (*common.ChainPower, error) {

	if len(actList) > 1 {
		return nil, xerrors.New("storage power actor count error")
	}

	if len(actList) == 0 {
		return nil, nil
	}

	powerActorState, err := power.Load(util.NewAPIIpldStore(ctx, node), &actList[0].Act)
	if err != nil {
		return nil, xerrors.Errorf("get power state (@ %s): %w", curTs.ParentState().String(), err)
	}

	totalPower, err := powerActorState.TotalPower()
	if err != nil {
		return nil, xerrors.Errorf("failed to compute total power: %w", err)
	}

	totalCommitted, err := powerActorState.TotalCommitted()
	if err != nil {
		return nil, xerrors.Errorf("failed to compute total committed: %w", err)
	}

	totalLocked, err := powerActorState.TotalLocked()
	if err != nil {
		return nil, xerrors.Errorf("failed to compute total locked: %w", err)
	}

	powerSmoothed, err := powerActorState.TotalPowerSmoothed()
	if err != nil {
		return nil, xerrors.Errorf("failed to determine smoothed power: %w", err)
	}

	// NOTE: this doesn't set new* fields. Previously, we
	// filled these using ThisEpoch* fields from the actor
	// state, but these fields are effectively internal
	// state and don't represent "new" power, as was
	// assumed.

	participatingMiners, totalMiners, err := powerActorState.MinerCounts()
	if err != nil {
		return nil, xerrors.Errorf("failed to count miners: %w", err)
	}

	var out common.ChainPower
	out.TotalRawBytes = totalPower.RawBytePower
	out.TotalQualityAdjustedBytes = totalPower.QualityAdjPower
	out.TotalRawBytesCommitted = totalCommitted.RawBytePower
	out.TotalQualityAdjustedBytesCommitted = totalCommitted.QualityAdjPower
	out.TotalPledgeCollateral = totalLocked
	out.QaPowerSmoothed = powerSmoothed
	out.MinerCountAboveMinimumPower = int64(participatingMiners)
	out.MinerCount = int64(totalMiners)

	return &out, nil
}
