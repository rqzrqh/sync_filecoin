package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func WriteExecutionTraceMessages(db *gorm.DB, tsID uint64, ts *types.TipSet, executionTraceMessages []*common.ExecutionTraceMessage, userMsgCidToMessageID map[string]uint64, systemMsgCidToMessageID map[string]uint64) error {
	height := int64(ts.Height())

	var messages []*model.ExecutionTraceMessage
	for _, v := range executionTraceMessages {

		messageID := uint64(0)
		exist := false
		msgType := model.MsgTypeUser

		if v.BelongToMsgType == common.MsgTypeUser {
			messageID, exist = userMsgCidToMessageID[v.BelongToMsgCid.String()]
			if !exist {
				log.Errorf("write executionTraceMessage but blong to user message id not found, cid=%v", v.BelongToMsgCid.String())
				return xerrors.New("write executionTraceMessage failed")
			}
			msgType = model.MsgTypeUser

		} else if v.BelongToMsgType == common.MsgTypeSystem {
			messageID, exist = systemMsgCidToMessageID[v.BelongToMsgCid.String()]
			if !exist {
				log.Errorf("write executionTraceMessage but blong to system message id not found, cid=%v", v.BelongToMsgCid.String())
				return xerrors.New("write executionTraceMessage failed")
			}
			msgType = model.MsgTypeSystem
		} else {
			panic(0)
		}

		msg := v.Message
		trace := v.Trace

		message := model.ExecutionTraceMessage{
			TipSetID:  tsID,
			Height:    height,
			MessageID: messageID,

			MsgType: msgType,

			// struct
			Level:       v.Level,
			Index:       v.Index,
			ParentIndex: v.ParentIndex,

			// msg
			Size:       msg.ChainLength(),
			Cid:        msg.Cid().String(),
			Version:    msg.Version,
			Nonce:      msg.Nonce,
			From:       msg.From.String(),
			To:         msg.To.String(),
			Value:      decimal.NewFromBigInt(msg.Value.Int, 0),
			GasLimit:   msg.GasLimit,
			GasFeeCap:  decimal.NewFromBigInt(msg.GasFeeCap.Int, 0),
			GasPremium: decimal.NewFromBigInt(msg.GasPremium.Int, 0),
			Method:     uint64(msg.Method),
			Params:     msg.Params,

			//ExecutionTrace
			Error:    trace.Error,
			Duration: trace.Duration.Nanoseconds(),

			// receipt
			ExitCode: int64(trace.Receipt.ExitCode),
			Return:   trace.Receipt.Return,
			GasUsed:  trace.Receipt.GasUsed,
		}
		messages = append(messages, &message)
	}

	if len(messages) > 0 {

		if err := db.CreateInBatches(&messages, 200).Error; err != nil {
			log.Errorw("WriteExecutionTraceMessages", "err", err, "height", height)
			return err
		}
	}

	return nil
}
