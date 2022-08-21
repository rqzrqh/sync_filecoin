package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/builtin/multisig"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"golang.org/x/xerrors"
)

// TODO now it can't work well, why? it is copy from filecoin-project/lily
func HandleMultiSigChanges(ctx context.Context, node api.FullNode, curTs *types.TipSet, actList []common.ActorInfo) ([]*common.MultisigTransaction, error) {

	var out []*common.MultisigTransaction

	if len(actList) == 0 {
		return out, nil
	}

	sto := store.ActorStore(ctx, blockstore.NewAPIBlockstore(node))

	for _, act := range actList {
		curState, err := multisig.Load(sto, &act.Act)
		if err != nil {
			return nil, xerrors.Errorf("loading current multisig state at head %s: %w", act.Act.Head, err)
		}

		var oldState multisig.State

		if oldAct, err := node.StateGetActor(ctx, act.Addr, curTs.Key()); err == nil {
			var err error
			oldState, err = multisig.Load(sto, oldAct)
			if err != nil {
				fmt.Println("load stat failed", act.Addr)
				return nil, err
			}
		} else {
			// if the actor exists in the current state and not in the parent state then the
			// actor was created in the current state.
			if err.Error() == types.ErrActorNotFound.Error() {
				oldState = curState
			} else {
				return nil, err
			}
		}

		if oldState == curState {
			if err := curState.ForEachPendingTxn(func(id int64, txn multisig.Transaction) error {
				// the ordering of this list must always be preserved as the 0th entry is the proposer.
				approved := make([]string, len(txn.Approved))
				for i, addr := range txn.Approved {
					approved[i] = addr.String()
				}
				out = append(out, &common.MultisigTransaction{
					MultisigID:    act.Addr,
					TransactionID: id,
					To:            txn.To,
					Value:         txn.Value,
					Method:        uint64(txn.Method),
					Params:        txn.Params,
					Approved:      approved,
				})
				return nil
			}); err != nil {
				return nil, err
			}
			continue
		}

		changes, err := getMultiSigChanges(&oldState, &curState)
		if err != nil {
			return nil, xerrors.Errorf("diffing pending transactions: %w", err)
		}

		for _, added := range changes.Added {
			approved := make([]string, len(added.Tx.Approved))
			for i, addr := range added.Tx.Approved {
				approved[i] = addr.String()
			}
			out = append(out, &common.MultisigTransaction{
				MultisigID:    act.Addr,
				TransactionID: added.TxID,
				To:            added.Tx.To,
				Value:         added.Tx.Value,
				Method:        uint64(added.Tx.Method),
				Params:        added.Tx.Params,
				Approved:      approved,
			})
		}

		for _, modded := range changes.Modified {
			approved := make([]string, len(modded.To.Approved))
			for i, addr := range modded.To.Approved {
				approved[i] = addr.String()
			}
			out = append(out, &common.MultisigTransaction{
				MultisigID:    act.Addr,
				TransactionID: modded.TxID,
				To:            modded.To.To,
				Value:         modded.To.Value,
				Method:        uint64(modded.To.Method),
				Params:        modded.To.Params,
				Approved:      approved,
			})
		}
	}

	return out, nil
}

func getMultiSigChanges(oldState *multisig.State, curState *multisig.State) (*multisig.PendingTransactionChanges, error) {
	start := time.Now()
	defer func() {
		log.Debugw("Get MultiSig Changes", "duration", time.Since(start).String())
	}()
	changed, err := multisig.DiffPendingTransactions(*oldState, *curState)
	if err != nil {
		return nil, err
	}
	return changed, nil
}
