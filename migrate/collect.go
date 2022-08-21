package migrate

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/ipfs/go-cid"
	"github.com/rqzrqh/sync_filecoin/common"
	"golang.org/x/sync/errgroup"
)

func CollectRawFullTipSetInfo(ctx context.Context, node api.FullNode, prevTsChangeAddressList []address.Address, syncPrevTsActorState bool, prevTs *types.TipSet, curTs *types.TipSet, curHeightKnownBlockMessages map[cid.Cid]*api.BlockMessages) ([]common.ActorInfo, []*api.BlockMessages, *api.ComputeStateOutput, []address.Address, error) {
	height := curTs.Height()
	tsk := curTs.Key()

	startTime := time.Now()
	defer func() {
		log.Debugw("collect", "duration", time.Since(startTime).String(), "tsk", tsk, "height", height)
	}()

	grp, _ := errgroup.WithContext(ctx)

	var prevAllChangedActors []common.ActorInfo
	curTsAllBlockMessages := make([]*api.BlockMessages, len(curTs.Blocks()))
	var curTsStateComputeOutout *api.ComputeStateOutput

	// 1. get prev changed actors by address
	if curTs.Height() > 0 && syncPrevTsActorState {
		mtx := sync.Mutex{}
		for _, addr := range prevTsChangeAddressList {

			a := addr

			grp.Go(func() error {

				// it seems need to filter address not exist
				prevActor, err := node.StateGetActor(ctx, a, curTs.Key())
				if err != nil {
					/*
						if err.Error() == types.ErrActorNotFound.Error() {
							log.Warnw("state get prev actor failed", "addr", a, "err", err)
							return nil
						}
					*/
					log.Warnw("state get prev actor failed, please take attention", "addr", a, "err", err)
					return nil
				}

				prevPrevActor, err := node.StateGetActor(ctx, a, prevTs.Key())
				if err != nil {
					//log.Warnw("state get prevprev actor failed, ignore", "addr", a, "err", err)
				}

				if prevPrevActor != nil {
					if prevPrevActor.Head.Equals(prevActor.Head) && prevPrevActor.Code.Equals(prevActor.Code) && prevPrevActor.Nonce == prevActor.Nonce && types.BigCmp(prevPrevActor.Balance, prevActor.Balance) == 0 {
						//log.Warnf("compare actor not change. addr:%v", a)
						return nil
					}
				}

				actorName := ActorNameByCode(prevActor.Code)

				act := common.ActorInfo{
					Act:         *prevActor,
					Addr:        a,
					ActorName:   actorName,
					ActorFamily: ActorFamily(actorName),
				}

				mtx.Lock()
				prevAllChangedActors = append(prevAllChangedActors, act)
				mtx.Unlock()

				return nil
			})
		}
		/*
			// chain-watch and FilMeta get changedActors in this way

			changes, err := GetActorChanges(ctx, node, prevTs, curTs)
			if err != nil {
				return err
			}

			changedActors, err := ParseActorChanges(ctx, node, curTs, changes)
			if err != nil {
				return err
			}

			prevAllChangedActors = changedActors
		*/
	}

	// 2. get user messages
	{
		mtx := sync.Mutex{}
		for i, b := range curTs.Blocks() {

			idx := i
			blkCid := b.Cid()

			if blockMessages, exist := curHeightKnownBlockMessages[blkCid]; exist {
				mtx.Lock()
				curTsAllBlockMessages[idx] = blockMessages
				mtx.Unlock()
				continue
			}

			grp.Go(func() error {

				startTime := time.Now()
				defer func() {
					log.Debugw("collect ChainGetBlockMessages", "duration", time.Since(startTime).String(), "cid", blkCid, "height", height, "idx", idx)
				}()

				blockMessages, err := node.ChainGetBlockMessages(ctx, blkCid)

				if err != nil {
					log.Errorw("collect ChainGetBlockMessages", err)
					return err
				}

				mtx.Lock()
				curTsAllBlockMessages[idx] = blockMessages
				mtx.Unlock()

				return nil
			})
		}
	}

	// 3. get StateCompute result
	grp.Go(func() error {
		startTime := time.Now()
		defer func() {
			log.Debugw("collect StateCompute", "duration", time.Since(startTime).String(), "tsk", tsk, "height", height)
		}()

		if curTs.Height() == 0 {
			return nil
		}

		sco, err := node.StateCompute(context.TODO(), curTs.Height(), nil, curTs.Key())
		if err != nil {
			log.Errorw("collect StateCompute", "err", err)
			return err
		}

		curTsStateComputeOutout = sco
		return nil
	})

	if err := grp.Wait(); err != nil {
		return nil, nil, nil, nil, err
	}

	// 4. get changed address for get changeActors at next linked tipset
	var curTsChangeAddressList []address.Address
	if curTs.Height() == 0 {
		var err error
		// TODO ,use height=1's tsk?
		curTsChangeAddressList, err = node.StateListActors(ctx, curTs.Key())
		if err != nil {
			return nil, nil, nil, nil, err
		}
	} else {
		changeAddrs := GetChangeActorsByMessages(curTsStateComputeOutout)
		for strAddr := range changeAddrs {
			addr, err := address.NewFromString(strAddr)
			if err != nil {
				log.Errorw("addr str to cid failed", "err", err)
				return nil, nil, nil, nil, err
			}
			curTsChangeAddressList = append(curTsChangeAddressList, addr)
		}
	}

	return prevAllChangedActors, curTsAllBlockMessages, curTsStateComputeOutout, curTsChangeAddressList, nil
}

func GetChangeActorsByMessages(sco *api.ComputeStateOutput) map[string]struct{} {

	actorSet := make(map[string]struct{})

	if sco == nil {
		return actorSet
	}

	for _, t := range sco.Trace {
		if t.Msg.From != builtin.SystemActorAddr && t.Msg.From != builtin.CronActorAddr {
			actorSet[t.Msg.From.String()] = struct{}{}
		}

		if t.Msg.To != builtin.SystemActorAddr && t.Msg.To != builtin.CronActorAddr && t.Error == "" {
			actorSet[t.Msg.To.String()] = struct{}{}
		}

		//fmt.Println("executedMessage from", t.Msg.From, "to", t.Msg.To, "method", t.Msg.Method, "value", t.Msg.Value, "error", t.Error, "end")
		getChangeActorsBySubCall(&t.ExecutionTrace, actorSet)
	}

	return actorSet
}

func getChangeActorsBySubCall(et *types.ExecutionTrace, actorSet map[string]struct{}) {
	if et.Msg.From != builtin.SystemActorAddr && et.Msg.From != builtin.CronActorAddr {
		actorSet[et.Msg.From.String()] = struct{}{}
	}

	if et.Msg.To != builtin.SystemActorAddr && et.Msg.To != builtin.CronActorAddr && et.Error == "" {
		actorSet[et.Msg.To.String()] = struct{}{}
	}
	//fmt.Println("traceMessage from", et.Msg.From, "to", et.Msg.To, "method", et.Msg.Method, "value", et.Msg.Value, "error", et.Error, "end")

	for _, v := range et.Subcalls {
		getChangeActorsBySubCall(&v, actorSet)
	}
}

func GetActorChanges(ctx context.Context, node api.FullNode, ts *types.TipSet, cts *types.TipSet) (map[string]types.Actor, error) {

	height := ts.Height()
	tsk := ts.Key()

	startTime := time.Now()
	defer func() {
		log.Debugw("collect getActorChanges", "duration", time.Since(startTime).String(), "tsk", tsk, "height", height)
	}()

	/*
		if pts.ParentState().Equals(bh.ParentStateRoot) {
			nullBlkMu.Lock()
			nullRounds = append(nullRounds, pts.Key())
			nullBlkMu.Unlock()
			// add log only?
		}

	*/

	// collect all actors that had state changes between the blockheader parent-state and its grandparent-state.
	// TODO: changes will contain deleted actors, this causes needless processing further down the pipeline, consider
	// a separate strategy for deleted actors
	changes, err := node.StateChangedActors(ctx, ts.ParentState(), cts.ParentState())
	if err != nil {
		log.Error(err)
		log.Errorw("collect getActorChanges", "parent_state", ts.ParentState(), "current_state", cts.ParentState())
		return nil, err
	}

	return changes, nil
}

func ParseActorChanges(ctx context.Context, node api.FullNode, ts *types.TipSet, changes map[string]types.Actor) ([]common.ActorInfo, error) {

	height := ts.Height()
	tsk := ts.Key()

	startTime := time.Now()
	defer func() {
		log.Debugw("collect parseActorChanges", "duration", time.Since(startTime).String(), "tsk", tsk, "height", height)
	}()

	var out []common.ActorInfo

	// record the state of all actors that have changed
	for a, act := range changes {
		//fmt.Println(a)
		//fmt.Println(act)

		act := act
		a := a

		// ignore actors that were deleted.
		has, err := node.ChainHasObj(ctx, act.Head)
		if err != nil {
			log.Error(err)
			log.Errorw("collect ChanHasObj", "actor_head", act.Head)
			return nil, err
		}
		if !has {
			continue
		}

		addr, err := address.NewFromString(a)
		if err != nil {
			log.Error(err)
			log.Errorw("collect NewFromString", "address_string", a)
			return nil, err
		}

		ast, err := node.StateReadState(ctx, addr, ts.Key())
		if err != nil {
			log.Error(err)
			log.Errorw("collect StateReadState", "address_string", a, "tsk", ts.Key())
			return nil, err
		}

		// TODO look here for an empty state, maybe thats a sign the actor was deleted?

		state, err := json.Marshal(ast.State)
		if err != nil {
			log.Error(err)
			return nil, err
		}

		// TODO use map?
		out = append(out, common.ActorInfo{
			Act:   act,
			Addr:  addr,
			State: string(state),
		})
	}

	return out, nil
}
