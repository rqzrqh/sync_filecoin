package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func setInvokeResult(modeMmessageInvokeResult *model.MessageInvokeResult, invokeResult *common.InvokeResult) {

	modeMmessageInvokeResult.GasUsed = decimal.NewFromBigInt(invokeResult.GasUsed.Int, 0)
	modeMmessageInvokeResult.BaseFeeBurn = decimal.NewFromBigInt(invokeResult.BaseFeeBurn.Int, 0)
	modeMmessageInvokeResult.OverEstimationBurn = decimal.NewFromBigInt(invokeResult.OverEstimationBurn.Int, 0)
	modeMmessageInvokeResult.MinerPenalty = decimal.NewFromBigInt(invokeResult.MinerPenalty.Int, 0)
	modeMmessageInvokeResult.MinerTip = decimal.NewFromBigInt(invokeResult.MinerTip.Int, 0)
	modeMmessageInvokeResult.Refund = decimal.NewFromBigInt(invokeResult.Refund.Int, 0)
	modeMmessageInvokeResult.TotalCost = decimal.NewFromBigInt(invokeResult.TotalCost.Int, 0)

	modeMmessageInvokeResult.GasRefund = decimal.NewFromBigInt(invokeResult.GasRefund.Int, 0)

	modeMmessageInvokeResult.Error = invokeResult.Error
	modeMmessageInvokeResult.Duration = invokeResult.Duration.Nanoseconds()

	// Receipt
	modeMmessageInvokeResult.ExitCode = int64(invokeResult.Receipt.ExitCode)
	modeMmessageInvokeResult.Return = invokeResult.Receipt.Return
}

func WriteMessageInvokeResults(db *gorm.DB, tsID uint64, ts *types.TipSet, userBlsMessages []*common.UserBlsMessage, userSecpMessages []*common.UserSecpMessage, systemMessages []*common.SystemMessage,
	userMsgCidToMessageID map[string]uint64, systemMsgCidToMessageID map[string]uint64) error {

	height := int64(ts.Height())

	var messageInvokeResults []*model.MessageInvokeResult
	for _, v := range userBlsMessages {
		msgID, exist := userMsgCidToMessageID[v.Msg.Cid().String()]
		if !exist {
			panic("msgid not found")
		}

		if v.Result != nil {
			modelMessageInvokeResult := model.MessageInvokeResult{
				Height:    height,
				TipSetID:  tsID,
				MessageID: msgID,
				MsgType:   model.MsgTypeUser,
			}

			setInvokeResult(&modelMessageInvokeResult, v.Result)
			messageInvokeResults = append(messageInvokeResults, &modelMessageInvokeResult)
		}
	}

	for _, v := range userSecpMessages {
		msgID, exist := userMsgCidToMessageID[v.Msg.Cid().String()]
		if !exist {
			panic("msgid not found")
		}

		if v.Result != nil {
			modelMessageInvokeResult := model.MessageInvokeResult{
				Height:    height,
				TipSetID:  tsID,
				MessageID: msgID,
				MsgType:   model.MsgTypeUser,
			}

			setInvokeResult(&modelMessageInvokeResult, v.Result)
			messageInvokeResults = append(messageInvokeResults, &modelMessageInvokeResult)
		}
	}

	for _, v := range systemMessages {
		msgID, exist := systemMsgCidToMessageID[v.Msg.Cid().String()]
		if !exist {
			panic("msgid not found")
		}

		if v.Result != nil {
			modelMessageInvokeResult := model.MessageInvokeResult{
				Height:    height,
				TipSetID:  tsID,
				MessageID: msgID,
				MsgType:   model.MsgTypeSystem,
			}

			setInvokeResult(&modelMessageInvokeResult, v.Result)
			messageInvokeResults = append(messageInvokeResults, &modelMessageInvokeResult)
		}
	}

	if len(messageInvokeResults) > 0 {
		if err := db.CreateInBatches(&messageInvokeResults, 500).Error; err != nil {
			log.Errorw("WriteMessageInvokeResults", "err", err, "height", height)
			return err
		}
	}

	return nil
}
