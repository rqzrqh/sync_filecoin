package migrate

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/util"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/reward"
	"github.com/filecoin-project/lotus/chain/types"
)

func HandleRewardChange(ctx context.Context, node api.FullNode, curTs *types.TipSet, actList []common.ActorInfo) (*common.ChainReward, error) {

	if len(actList) > 1 {
		return nil, xerrors.New("reward actor count error")
	}

	if len(actList) == 0 {
		return nil, nil
	}

	rewardActorState, err := reward.Load(util.NewAPIIpldStore(ctx, node), &actList[0].Act)
	if err != nil {
		return nil, xerrors.Errorf("read state obj (@ %s): %w", curTs.ParentState().String(), err)
	}

	cumSumBaselinePower, err := rewardActorState.CumsumBaseline()
	if err != nil {
		return nil, xerrors.Errorf("getting cumsum baseline power (@ %s): %w", curTs.ParentState().String(), err)
	}

	cumSumRealizedPower, err := rewardActorState.CumsumRealized()
	if err != nil {
		return nil, xerrors.Errorf("getting cumsum realized power (@ %s): %w", curTs.ParentState().String(), err)
	}

	effectiveNetworkTime, err := rewardActorState.EffectiveNetworkTime()
	if err != nil {
		return nil, xerrors.Errorf("getting effective network time (@ %s): %w", curTs.ParentState().String(), err)
	}

	effectiveBaselinePower, err := rewardActorState.EffectiveBaselinePower()
	if err != nil {
		return nil, xerrors.Errorf("getting effective baseline power (@ %s): %w", curTs.ParentState().String(), err)
	}

	totalMinedReward, err := rewardActorState.TotalStoragePowerReward()
	if err != nil {
		return nil, xerrors.Errorf("getting  total mined (@ %s): %w", curTs.ParentState().String(), err)
	}

	newBaselinePower, err := rewardActorState.ThisEpochBaselinePower()
	if err != nil {
		return nil, xerrors.Errorf("getting this epoch baseline power (@ %s): %w", curTs.ParentState().String(), err)
	}

	newBaseReward, err := rewardActorState.ThisEpochReward()
	if err != nil {
		return nil, xerrors.Errorf("getting this epoch baseline power (@ %s): %w", curTs.ParentState().String(), err)
	}

	newSmoothingEstimate, err := rewardActorState.ThisEpochRewardSmoothed()
	if err != nil {
		return nil, xerrors.Errorf("getting this epoch baseline power (@ %s): %w", curTs.ParentState().String(), err)
	}

	var out common.ChainReward
	out.CumSumBaselinePower = cumSumBaselinePower
	out.CumSumRealizedPower = cumSumRealizedPower
	out.EffectiveNetworkTime = effectiveNetworkTime
	out.EffectiveBaselinePower = effectiveBaselinePower
	out.TotalMinedReward = totalMinedReward
	out.NewBaselinePower = newBaselinePower
	out.NewBaseReward = newBaseReward
	out.NewSmoothingEstimate = newSmoothingEstimate

	return &out, nil
}
