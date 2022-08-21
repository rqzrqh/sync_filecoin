package migrate

import (
	"context"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/reward"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	builtin4 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	builtin5 "github.com/filecoin-project/specs-actors/v5/actors/builtin"
	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	builtin7 "github.com/filecoin-project/specs-actors/v7/actors/builtin"

	common "github.com/rqzrqh/sync_filecoin/common"
)

type ActorProcessResult struct {
	chainPower          *common.ChainPower
	chainReward         *common.ChainReward
	chainInitActor      *common.ChainInitActor
	marketDealProposals []*common.MarketDealProposal
	marketDealStates    []*common.MarketDealState
	paymentChannels     []*common.PaymentChannel

	miners               []*common.Miner
	minerPowers          []*common.MinerPower
	sectorInfos          []*common.SectorInfo
	sectorPrecommitInfos []*common.SectorPrecommitInfo
	minerSectorEvents    []*common.MinerSectorEvent
	sectorDealEvents     []*common.SectorDealEvent
	multiSigTxs          []*common.MultisigTransaction
}

type ScoProcessResult struct {
	userBlsMessages  []*common.UserBlsMessage
	userSecpMessages []*common.UserSecpMessage
	systemMessages   []*common.SystemMessage

	executionTraceMessages []*common.ExecutionTraceMessage

	totalReward  abi.TokenAmount
	blockRewards []abi.TokenAmount
	penalty      abi.TokenAmount
}

type MessageProcessResult struct {
	allBlockMessageSet   map[cid.Cid]int
	unProcessedMsgCidSet map[cid.Cid]struct{}
}

func Process(ctx context.Context, node api.FullNode, prevTs *types.TipSet, curTs *types.TipSet, prevTsChangedActors []common.ActorInfo, curTsAllBlockMessages []*api.BlockMessages,
	curTsSco *api.ComputeStateOutput, curTsChangeAddressList []address.Address) (*common.FullTipSet, error) {

	height := curTs.Height()
	tsk := curTs.Key()

	start := time.Now()
	defer func() {
		log.Debugw("process", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	var messageProcessResult *MessageProcessResult
	var actorProcessResult *ActorProcessResult
	var scoProcessResult *ScoProcessResult

	grp, _ := errgroup.WithContext(ctx)

	if curTs.Height() != 0 {
		grp.Go(func() error {
			result, err := processActors(ctx, node, prevTs, curTs, prevTsChangedActors)
			if err != nil {
				return err
			}

			actorProcessResult = result
			return nil
		})
	}

	grp.Go(func() error {
		result := processStateComputeResult(ctx, curTs, curTsAllBlockMessages, curTsSco)
		scoProcessResult = result
		return nil
	})

	if err := grp.Wait(); err != nil {
		log.Errorw("process failed", err)
		return nil, err
	}

	messageProcessResult = processMessages(ctx, curTs, curTsAllBlockMessages, scoProcessResult.userBlsMessages, scoProcessResult.userSecpMessages)

	var fts common.FullTipSet
	fts.PrevTs = prevTs
	fts.CurTs = curTs

	// prevTs
	if curTs.Height() != 0 {
		fts.ChangedActors = prevTsChangedActors
		fts.ChainPower = actorProcessResult.chainPower
		fts.ChainReward = actorProcessResult.chainReward

		fts.ChainInitActor = actorProcessResult.chainInitActor
		fts.MarketDealProposals = actorProcessResult.marketDealProposals
		fts.MarketDealStates = actorProcessResult.marketDealStates
		fts.PaymentChannels = actorProcessResult.paymentChannels

		fts.Miners = actorProcessResult.miners
		fts.MinerPowers = actorProcessResult.minerPowers
		fts.SectorInfos = actorProcessResult.sectorInfos
		fts.SectorPrecommitInfos = actorProcessResult.sectorPrecommitInfos
		fts.MinerSectorEvents = actorProcessResult.minerSectorEvents
		fts.SectorDealEvents = actorProcessResult.sectorDealEvents
		fts.MultiSigTxs = actorProcessResult.multiSigTxs
	}

	// curTs
	fts.AllBlockMessages = curTsAllBlockMessages
	fts.AllBlockMessageSet = messageProcessResult.allBlockMessageSet
	fts.UnProcessedMsgCidSet = messageProcessResult.unProcessedMsgCidSet

	fts.UserBlsMessages = scoProcessResult.userBlsMessages
	fts.UserSecpMessages = scoProcessResult.userSecpMessages
	fts.SystemMessages = scoProcessResult.systemMessages

	fts.ExecutionTraceMesages = scoProcessResult.executionTraceMessages

	fts.TotalReward = scoProcessResult.totalReward
	fts.BlockRewards = scoProcessResult.blockRewards
	fts.Penalty = scoProcessResult.penalty

	// message count,gas costs

	fts.CurTsChangeAddressList = curTsChangeAddressList

	return &fts, nil
}

func processActors(ctx context.Context, node api.FullNode, prevTs *types.TipSet, curTs *types.TipSet, actorChanges []common.ActorInfo) (*ActorProcessResult, error) {

	height := curTs.Height()
	tsk := curTs.Key()

	start := time.Now()
	defer func() {
		log.Debugw("processActors ", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	var actorResult ActorProcessResult

	storageMarketActorList := make([]common.ActorInfo, 0)
	storageMinerActorList := make([]common.ActorInfo, 0)
	rewardActorList := make([]common.ActorInfo, 0)
	storagePowerActorList := make([]common.ActorInfo, 0)
	initActorList := make([]common.ActorInfo, 0)
	accountActorList := make([]common.ActorInfo, 0)
	paymentChannelActorList := make([]common.ActorInfo, 0)
	multiSigActorList := make([]common.ActorInfo, 0)
	verifiedRegistryActorList := make([]common.ActorInfo, 0)

	for _, v := range actorChanges {
		//log.Debugw("###Collected Actor Changes", "name", v.Name, "addr", v.Addr)
		switch v.Act.Code {
		case builtin0.StorageMarketActorCodeID, builtin2.StorageMarketActorCodeID, builtin0.StorageMarketActorCodeID, builtin3.StorageMarketActorCodeID, builtin4.StorageMarketActorCodeID,
			builtin5.StorageMarketActorCodeID, builtin6.StorageMarketActorCodeID, builtin7.StorageMarketActorCodeID:
			storageMarketActorList = append(storageMarketActorList, v)
		case builtin0.StorageMinerActorCodeID, builtin2.StorageMinerActorCodeID, builtin3.StorageMinerActorCodeID, builtin4.StorageMinerActorCodeID, builtin5.StorageMinerActorCodeID,
			builtin6.StorageMinerActorCodeID, builtin7.StorageMinerActorCodeID:
			storageMinerActorList = append(storageMinerActorList, v)
		case builtin0.RewardActorCodeID, builtin2.RewardActorCodeID, builtin3.RewardActorCodeID, builtin4.RewardActorCodeID, builtin5.RewardActorCodeID, builtin6.RewardActorCodeID,
			builtin7.RewardActorCodeID:
			rewardActorList = append(rewardActorList, v)
		case builtin0.StoragePowerActorCodeID, builtin2.StoragePowerActorCodeID, builtin3.StoragePowerActorCodeID, builtin4.StoragePowerActorCodeID, builtin5.StoragePowerActorCodeID,
			builtin6.StoragePowerActorCodeID, builtin7.StoragePowerActorCodeID:
			storagePowerActorList = append(storagePowerActorList, v)
		case builtin0.InitActorCodeID, builtin2.InitActorCodeID, builtin3.InitActorCodeID, builtin4.InitActorCodeID, builtin5.InitActorCodeID, builtin6.InitActorCodeID, builtin7.InitActorCodeID:
			initActorList = append(initActorList, v)
		case builtin0.AccountActorCodeID, builtin2.AccountActorCodeID, builtin3.AccountActorCodeID, builtin4.AccountActorCodeID, builtin5.AccountActorCodeID, builtin6.AccountActorCodeID,
			builtin7.AccountActorCodeID:
			accountActorList = append(accountActorList, v)
		case builtin0.PaymentChannelActorCodeID, builtin2.PaymentChannelActorCodeID, builtin3.PaymentChannelActorCodeID, builtin4.PaymentChannelActorCodeID, builtin5.PaymentChannelActorCodeID,
			builtin6.PaymentChannelActorCodeID, builtin7.PaymentChannelActorCodeID:
			paymentChannelActorList = append(paymentChannelActorList, v)
		case builtin0.MultisigActorCodeID, builtin2.MultisigActorCodeID, builtin3.MultisigActorCodeID, builtin4.MultisigActorCodeID, builtin5.MultisigActorCodeID, builtin6.MultisigActorCodeID,
			builtin7.MultisigActorCodeID:
			multiSigActorList = append(multiSigActorList, v)
		case builtin0.VerifiedRegistryActorCodeID, builtin2.VerifiedRegistryActorCodeID, builtin3.VerifiedRegistryActorCodeID, builtin4.VerifiedRegistryActorCodeID, builtin5.VerifiedRegistryActorCodeID,
			builtin6.VerifiedRegistryActorCodeID, builtin7.VerifiedRegistryActorCodeID:
			verifiedRegistryActorList = append(verifiedRegistryActorList, v)
		default:
			log.Errorw("not support", "Name", builtin.ActorNameByCode(v.Act.Code), "addr", v.Addr, "code", v.Act.Code)
			panic("maybe filecoin upgrade, fix it and ensure any actors are recognized")
		}
	}

	grp, _ := errgroup.WithContext(ctx)
	/*
		//too slow?
		if len(storageMarketActorList) > 0 {
			grp.Go(func() error {
				startTime := time.Now()
				defer func() {
					log.Debugw("process Market", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
				}()

				proposals, states, err := HandleStorageMarketChanges(ctx, node, ts, storageMarketActorList)
				if err != nil {
					log.Errorf("Failed to handle market changes: %s", err)
					return err
				}
				actorResult.marketDealProposals = proposals
				actorResult.marketDealStates = states
				return nil
			})
		}
	*/

	if len(storageMinerActorList) > 0 {
		grp.Go(func() error {

			startTime := time.Now()
			defer func() {
				log.Debugw("process Miners", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
			}()

			minerPowers, miners, sectorInfos, sectorPrecommitInfos, minerSectorEvents, sectorDealEvents, err := HandleStorageMinerChanges(ctx, node, prevTs, curTs, &storagePowerActorList[0].Act, storageMinerActorList)
			if err != nil {
				log.Errorf("Failed to handle miner changes: %s", err)
				return err
			}
			actorResult.minerPowers = minerPowers
			actorResult.miners = miners
			actorResult.sectorInfos = sectorInfos
			actorResult.sectorPrecommitInfos = sectorPrecommitInfos
			actorResult.minerSectorEvents = minerSectorEvents
			actorResult.sectorDealEvents = sectorDealEvents
			return nil
		})
	}

	if len(rewardActorList) > 0 {
		grp.Go(func() error {
			startTime := time.Now()
			defer func() {
				log.Debugw("process Reward Actors", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
			}()

			reward, err := HandleRewardChange(ctx, node, curTs, rewardActorList)
			if err != nil {
				log.Errorf("Failed to handle reward changes: %s", err)
				return err
			}
			actorResult.chainReward = reward
			return nil
		})
	}

	if len(storagePowerActorList) > 0 {
		grp.Go(func() error {
			startTime := time.Now()
			defer func() {
				log.Debugw("process Power Actors", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
			}()

			power, err := HandleStoragePowerChange(ctx, node, curTs, storagePowerActorList)
			if err != nil {
				log.Errorf("Failed to handle power actor changes: %s", err)
				return err
			}
			actorResult.chainPower = power
			return nil
		})
	}

	if len(initActorList) > 0 {
		grp.Go(func() error {
			startTime := time.Now()
			defer func() {
				log.Debugw("process Init Actors", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
			}()

			chainInitActor, err := HandleInitActorChanges(ctx, node, curTs, initActorList, actorChanges)
			if err != nil {
				log.Errorf("Failed to handle init actor changes: %s", err)
				return err
			}
			actorResult.chainInitActor = chainInitActor
			return nil
		})
	}

	if len(accountActorList) > 0 {
		grp.Go(func() error {
			startTime := time.Now()
			defer func() {
				log.Debugw("process Account", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
			}()

			err := HandleAccountChanges(ctx, accountActorList)
			if err != nil {
				log.Errorf("Failed to handle account actor changes: %s", err)
				return err
			}
			return nil
		})
	}

	if len(paymentChannelActorList) > 0 {
		grp.Go(func() error {
			startTime := time.Now()
			defer func() {
				log.Debugw("process Payment Channel", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
			}()

			paymentChannels, err := HandlePaymentChannelChanges(ctx, node, paymentChannelActorList)
			if err != nil {
				log.Errorf("Failed to handle paych actor changes: %s", err)
				return err
			}
			actorResult.paymentChannels = paymentChannels
			return nil
		})
	}

	if len(multiSigActorList) > 0 {
		grp.Go(func() error {
			startTime := time.Now()
			defer func() {
				log.Debugw("process multi sig", "duration", time.Since(startTime).String(), "height", height, "tsk", tsk)
			}()

			multiSigTxs, err := HandleMultiSigChanges(ctx, node, curTs, multiSigActorList)
			if err != nil {
				log.Errorf("Failed to handle MultiSig actor changes: %s", err)
				return err
			}
			actorResult.multiSigTxs = multiSigTxs

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		log.Errorw("processActors failed", err)
		return nil, err
	}

	return &actorResult, nil
}

func processStateComputeResult(ctx context.Context, ts *types.TipSet, allBlockMessages []*api.BlockMessages, sco *api.ComputeStateOutput) *ScoProcessResult {
	height := ts.Height()
	tsk := ts.Key()

	start := time.Now()
	defer func() {
		log.Debugw("processStateCompute ", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	var userBlsMessages []*common.UserBlsMessage
	var userSecpMessages []*common.UserSecpMessage
	var systemMessages []*common.SystemMessage

	var executionTraceMessages []*common.ExecutionTraceMessage
	var rewardTraces []*types.ExecutionTrace

	if sco == nil {
		// TODO message?
		blockRewards := make([]abi.TokenAmount, len(ts.Blocks()))
		for idx, _ := range ts.Blocks() {
			blockRewards[idx] = abi.NewTokenAmount(0)
		}

		scoProcessResult := ScoProcessResult{
			blockRewards: blockRewards,
		}

		return &scoProcessResult
	}

	blsMessageSet := make(map[cid.Cid]*types.Message)
	secpMessageSet := make(map[cid.Cid]*types.SignedMessage)

	for _, blockMessages := range allBlockMessages {
		for _, blsMsg := range blockMessages.BlsMessages {
			blsMessageSet[blsMsg.Cid()] = blsMsg
		}

		for _, secpMsg := range blockMessages.SecpkMessages {
			secpMessageSet[secpMsg.Cid()] = secpMsg
		}
	}

	for _, t := range sco.Trace {

		// when msg is user secp message, t.MsgCid != t.Msg.Cid()

		//fmt.Println("##sco", t.MsgCid, t.Msg.Cid())

		msgCid := t.MsgCid

		invokeResult := common.InvokeResult{
			MsgID: msgCid,

			GasUsed:            t.GasCost.GasUsed,
			BaseFeeBurn:        t.GasCost.BaseFeeBurn,
			OverEstimationBurn: t.GasCost.OverEstimationBurn,
			MinerPenalty:       t.GasCost.MinerPenalty,
			MinerTip:           t.GasCost.MinerTip,
			Refund:             t.GasCost.Refund,
			TotalCost:          t.GasCost.TotalCost,

			GasRefund: big.Sub(t.Msg.RequiredFunds(), t.GasCost.TotalCost),

			Error:    t.Error,
			Duration: t.Duration,

			Receipt: t.MsgRct,
		}

		msgType := common.MsgTypeUser

		// TODO add multi times？
		if a, exist := blsMessageSet[msgCid]; exist {
			msg := common.UserBlsMessage{
				Msg:    a,
				Result: &invokeResult,
			}
			userBlsMessages = append(userBlsMessages, &msg)
			delete(blsMessageSet, msgCid)
			msgType = common.MsgTypeUser

		} else if b, exist := secpMessageSet[msgCid]; exist {
			msg := common.UserSecpMessage{
				Msg:    b,
				Result: &invokeResult,
			}
			userSecpMessages = append(userSecpMessages, &msg)
			delete(secpMessageSet, msgCid)
			msgType = common.MsgTypeUser

		} else {
			msg := common.SystemMessage{
				Msg:    t.Msg,
				Result: &invokeResult,
			}
			systemMessages = append(systemMessages, &msg)
			msgType = common.MsgTypeSystem
		}

		level := 0
		parentIndex := -1
		counter := parentIndex
		msgMessages := flattenExecutionTrace(msgCid, msgType, &t.ExecutionTrace, nil, level, parentIndex, &counter)

		executionTraceMessages = append(executionTraceMessages, msgMessages...)

		if t.ExecutionTrace.Msg.Method == reward.Methods.AwardBlockReward && t.ExecutionTrace.Msg.To == reward.Address {
			rewardTraces = append(rewardTraces, &t.ExecutionTrace)
		}
	}

	for _, v := range blsMessageSet {
		msg := common.UserBlsMessage{
			Msg:    v,
			Result: nil,
		}
		userBlsMessages = append(userBlsMessages, &msg)
	}
	for _, v := range secpMessageSet {
		msg := common.UserSecpMessage{
			Msg:    v,
			Result: nil,
		}
		userSecpMessages = append(userSecpMessages, &msg)
	}

	// calc reward
	var totalReward abi.TokenAmount
	var blockRewards []abi.TokenAmount
	var penalty abi.TokenAmount

	for _, blk := range ts.Blocks() {
		var blockReward abi.TokenAmount
		var blockPenalty abi.TokenAmount
		for _, rewardTrace := range rewardTraces {
			for _, sub := range rewardTrace.Subcalls {
				switch sub.Msg.To {
				case builtin.BurntFundsActorAddr:
					blockPenalty = sub.Msg.Value
				case blk.Miner:
					blockReward = sub.Msg.Value
				}
			}
		}
		//fmt.Println("reward and penalty=", blockReward, blockPenalty)

		// which block

		//rewardPenalty.totalReward.Add(reward)
		totalReward = blockReward
		blockRewards = append(blockRewards, blockReward)
		penalty = blockPenalty
	}

	scoProcessResult := ScoProcessResult{
		userBlsMessages:        userBlsMessages,
		userSecpMessages:       userSecpMessages,
		systemMessages:         systemMessages,
		executionTraceMessages: executionTraceMessages,
		totalReward:            totalReward,
		blockRewards:           blockRewards,
		penalty:                penalty,
	}

	return &scoProcessResult
}

func flattenExecutionTrace(msgCid cid.Cid, msgType uint8, et *types.ExecutionTrace, gasCharge *types.GasTrace, level int, parentIndex int, counter *int) []*common.ExecutionTraceMessage {
	var executionTracMmessages []*common.ExecutionTraceMessage

	*counter += 1

	index := *counter
	{
		executionTrace := common.ExecutionTrace{
			Error:     et.Error,
			Duration:  et.Duration,
			GasCharge: gasCharge,

			Receipt: et.MsgRct,
		}

		etMsg := common.ExecutionTraceMessage{
			BelongToMsgCid:  msgCid,
			BelongToMsgType: msgType,

			Level:       level,
			Index:       index,
			ParentIndex: parentIndex,

			Message: et.Msg,
			Trace:   &executionTrace,
		}

		executionTracMmessages = append(executionTracMmessages, &etMsg)
	}

	//fmt.Println("gassize=", len(et.GasCharges))
	//fmt.Println("sub=", len(et.Subcalls))

	// et.GasCharges count not equal with subcalls

	for _, v := range et.Subcalls {
		subMessages := flattenExecutionTrace(msgCid, msgType, &v, nil, level+1, index, counter)
		executionTracMmessages = append(executionTracMmessages, subMessages...)
	}

	return executionTracMmessages
}

func processMessages(ctx context.Context, ts *types.TipSet, allBlockMessages []*api.BlockMessages, userBlsMessages []*common.UserBlsMessage, userSecpMessages []*common.UserSecpMessage) *MessageProcessResult {
	height := ts.Height()
	tsk := ts.Key()

	start := time.Now()
	defer func() {
		log.Debugw("processMessages ", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	allBlockMessageSet := make(map[cid.Cid]int)
	unProcessedMsgCidSet := make(map[cid.Cid]struct{})

	for _, blockMessages := range allBlockMessages {
		for _, msgCid := range blockMessages.Cids {
			count, exist := allBlockMessageSet[msgCid]
			if exist {
				allBlockMessageSet[msgCid] = count + 1
			} else {
				allBlockMessageSet[msgCid] = 1
			}
		}
	}

	for _, v := range userBlsMessages {
		if v.Result == nil {
			unProcessedMsgCidSet[v.Msg.Cid()] = struct{}{}
		}
	}
	for _, v := range userSecpMessages {
		if v.Result == nil {
			unProcessedMsgCidSet[v.Msg.Cid()] = struct{}{}
		}
	}

	result := MessageProcessResult{
		allBlockMessageSet:   allBlockMessageSet,
		unProcessedMsgCidSet: unProcessedMsgCidSet,
	}

	return &result
}
