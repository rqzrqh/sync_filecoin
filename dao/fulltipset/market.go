package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteMarketDealProposals(db *gorm.DB, tsID uint64, ts *types.TipSet, marketDealProposals []*common.MarketDealProposal) error {

	height := int64(ts.Height())

	var modelDealProposals []model.MarketDealProposal
	for _, proposal := range marketDealProposals {
		modelDealProposal := model.MarketDealProposal{
			TipSetID:          tsID,
			Height:            height,
			DealID:            uint64(proposal.DealID),
			PieceCID:          proposal.PieceCID.String(),
			PieceSize:         uint64(proposal.PieceSize),
			PieceSizeUnpadded: uint64(proposal.PieceSize.Unpadded()),
			VerifiedDeal:      proposal.VerifiedDeal,
			Client:            proposal.Client.String(),
			Provider:          proposal.Provider.String(),
			StartEpoch:        int64(proposal.StartEpoch),
			EndEpoch:          int64(proposal.EndEpoch),
			//SlashedEpoch
			StoragePricePerEpoch: decimal.NewFromBigInt(proposal.StoragePricePerEpoch.Int, 0),
			ProviderCollateral:   decimal.NewFromBigInt(proposal.ProviderCollateral.Int, 0),
			ClientCollateral:     decimal.NewFromBigInt(proposal.ClientCollateral.Int, 0),
		}
		modelDealProposals = append(modelDealProposals, modelDealProposal)
	}

	if len(modelDealProposals) > 0 {
		if err := db.CreateInBatches(&modelDealProposals, 100).Error; err != nil {
			log.Errorw("WriteMarketDealProposals", "err", err, "height", height)
			return err
		}
	}

	return nil
}

func WriteMarketDealStates(db *gorm.DB, tsID uint64, ts *types.TipSet, marketDealStates []*common.MarketDealState) error {
	height := int64(ts.Height())

	var modelDealStates []model.MarketDealState
	for _, state := range marketDealStates {
		modelDealState := model.MarketDealState{
			TipSetID:         tsID,
			Height:           height,
			DealID:           uint64(state.DealID),
			SectorStartEpoch: int64(state.SectorStartEpoch),
			LastUpdatedEpoch: int64(state.LastUpdatedEpoch),
			SlashEpoch:       int64(state.SlashEpoch),
		}
		modelDealStates = append(modelDealStates, modelDealState)
	}

	if len(modelDealStates) > 0 {

		if err := db.CreateInBatches(&modelDealStates, 100).Error; err != nil {
			log.Errorw("WriteMarketDealStates", "err", err, "height", height)
			return err
		}
	}

	return nil
}
