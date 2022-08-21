package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteSectorInfos(db *gorm.DB, tsID uint64, ts *types.TipSet, allSectorInfos []*common.SectorInfo) error {
	height := int64(ts.Height())

	var sectorInfos []model.SectorInfo
	for _, v := range allSectorInfos {
		sectorInfo := model.SectorInfo{
			TipSetID:              tsID,
			Height:                height,
			MinerID:               v.MinerID.String(),
			SectorID:              uint64(v.SectorID),
			SealedCid:             v.SealedCid.String(),
			ActivationEpoch:       int64(v.ActivationEpoch),
			ExpirationEpoch:       int64(v.ExpirationEpoch),
			DealWeight:            decimal.NewFromBigInt(v.DealWeight.Int, 0),
			VerifiedDealWeight:    decimal.NewFromBigInt(v.VerifiedDealWeight.Int, 0),
			InitialPledge:         decimal.NewFromBigInt(v.InitialPledge.Int, 0),
			ExpectedDayReward:     decimal.NewFromBigInt(v.ExpectedDayReward.Int, 0),
			ExpectedStoragePledge: decimal.NewFromBigInt(v.ExpectedStoragePledge.Int, 0),
		}
		sectorInfos = append(sectorInfos, sectorInfo)
	}

	if len(sectorInfos) > 0 {

		if err := db.CreateInBatches(&sectorInfos, 500).Error; err != nil {
			log.Errorw("WriteSectorInfos", "err", err, "height", height)
			return err
		}
	}

	return nil
}

func WriteSectorPrecommitInfos(db *gorm.DB, tsID uint64, ts *types.TipSet, allSectorPrecommitInfos []*common.SectorPrecommitInfo) error {
	height := int64(ts.Height())

	var sectorPrecommitInfos []model.SectorPrecommitInfo
	for _, v := range allSectorPrecommitInfos {
		sectorPrecommitInfo := model.SectorPrecommitInfo{
			TipSetID:               tsID,
			Height:                 height,
			MinerID:                v.MinerID.String(),
			SectorID:               uint64(v.SectorID),
			SealedCid:              v.SealedCid.String(),
			SealRandEpoch:          int64(v.SealRandEpoch),
			ExpirationEpoch:        int64(v.ExpirationEpoch),
			PrecommitDeposit:       decimal.NewFromBigInt(v.PrecommitDeposit.Int, 0),
			PrecommitEpoch:         int64(v.PrecommitEpoch),
			DealWeight:             decimal.NewFromBigInt(v.DealWeight.Int, 0),
			VerifiedDealWeight:     decimal.NewFromBigInt(v.VerifiedDealWeight.Int, 0),
			IsReplaceCapacity:      v.IsReplaceCapacity,
			ReplaceSectorDeadline:  v.ReplaceSectorDeadline,
			ReplaceSectorPartition: v.ReplaceSectorPartition,
			ReplaceSectorNumber:    uint64(v.ReplaceSectorNumber),
		}
		sectorPrecommitInfos = append(sectorPrecommitInfos, sectorPrecommitInfo)
	}

	if len(sectorPrecommitInfos) > 0 {

		if err := db.CreateInBatches(&sectorPrecommitInfos, 500).Error; err != nil {
			log.Errorw("WriteSectorPrecommitInfos", "err", err, "height", height)
			return err
		}
	}

	return nil
}

func WriteMinerSectorEvents(db *gorm.DB, tsID uint64, ts *types.TipSet, allMinerSectorEvents []*common.MinerSectorEvent) error {
	height := int64(ts.Height())

	var minerSectorEvents []model.MinerSectorEvent
	for _, v := range allMinerSectorEvents {
		minerSectorEvent := model.MinerSectorEvent{
			TipSetID: tsID,
			Height:   height,
			MinerID:  v.MinerID.String(),
			SectorID: v.SectorID,
			Event:    model.SectorLifecycleEvent(v.Event),
		}
		minerSectorEvents = append(minerSectorEvents, minerSectorEvent)
	}

	if len(minerSectorEvents) > 0 {

		if err := db.CreateInBatches(&minerSectorEvents, 500).Error; err != nil {
			log.Errorw("WriteMinerSectorEvents", "err", err, "height", height)
			return err
		}
	}

	return nil
}

func WriteSectorDealEvents(db *gorm.DB, tsID uint64, ts *types.TipSet, allSectorDealEvents []*common.SectorDealEvent) error {
	height := int64(ts.Height())

	var sectorDealEvents []model.SectorDealEvent
	for _, v := range allSectorDealEvents {
		sectorDealEvent := model.SectorDealEvent{
			TipSetID: tsID,
			Height:   height,
			DealID:   uint64(v.DealID),
			MinerID:  v.MinerID.String(),
			SectorID: v.SectorID,
		}
		sectorDealEvents = append(sectorDealEvents, sectorDealEvent)
	}

	if len(sectorDealEvents) > 0 {

		if err := db.CreateInBatches(&sectorDealEvents, 500).Error; err != nil {
			log.Errorw("WriteSectorDealEvents", "err", err, "height", height)
			return err
		}
	}

	return nil
}
