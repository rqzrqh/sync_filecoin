package local_chain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rqzrqh/sync_filecoin/dao"
	"github.com/rqzrqh/sync_filecoin/model"
	"go.uber.org/atomic"
	"gorm.io/gorm"
)

type CacheChainHead struct {
	height int64
}

type CacheChain struct {
	ctx                  context.Context
	db                   *gorm.DB
	rds                  *redis.Client
	cacheChainHeadNotify chan CacheChainHead
}

func newCacheChain(ctx context.Context, db *gorm.DB, rds *redis.Client, cacheChainHeadNotify chan CacheChainHead) *CacheChain {
	return &CacheChain{
		ctx:                  ctx,
		db:                   db,
		rds:                  rds,
		cacheChainHeadNotify: cacheChainHeadNotify,
	}
}

func (c *CacheChain) getChainHead() *dao.HeadChainInfo {
	var modelReversibleTipSet model.ReversibleTipSet
	result := c.db.Model(&model.ReversibleTipSet{}).Where("status = ?", model.TipSetStatusValid).Order("parent_weight DESC").Take(&modelReversibleTipSet)
	if result.Error != nil {
		fmt.Println(result.Error)
		panic("db get failed")
	}

	if result.RowsAffected != 1 {
		panic("db get count is 0")
	}

	chainHead := &dao.HeadChainInfo{
		Height:       modelReversibleTipSet.Height,
		ParentHeight: modelReversibleTipSet.ParentHeight,
		ID:           modelReversibleTipSet.TipSetID,
		ParentID:     modelReversibleTipSet.ParentTipSetID,
	}

	return chainHead
}

func (c *CacheChain) getBranchFromHeadToHeadChain(tsID uint64) []*dao.HeadChainInfo {

	var oldHeadChainList []*dao.HeadChainInfo

	// get rollback tipset, must select from tipset table, because cache data could lag far behind db in extreme cases.
	for {
		var modelTipSet model.TipSet
		result := c.db.Model(&model.TipSet{}).Where("id = ? AND is_valid = true", tsID).Take(&modelTipSet)
		if result.Error != nil {
			panic("sxxxxx")
		}

		if result.RowsAffected != 1 {
			panic("sxxxxx222")
		}

		if modelTipSet.InHeadChain {
			break
		}

		ts := &dao.HeadChainInfo{
			Height:       modelTipSet.Height,
			ParentHeight: modelTipSet.ParentHeight,
			ID:           modelTipSet.ID,
			ParentID:     modelTipSet.ParentTipSetID,
		}

		oldHeadChainList = append(oldHeadChainList, ts)

		tsID = modelTipSet.ParentTipSetID
	}

	return oldHeadChainList
}

func (c *CacheChain) getHeadChain(height int64, tsID uint64) []*dao.HeadChainInfo {
	var modelTipSetList []model.TipSet
	result := c.db.Model(&model.TipSet{}).Where("height > ? AND is_valid = true and in_head_chain = true", height).Order("height ASC").Limit(1).Find(&modelTipSetList)
	if result.Error != nil {
		panic("sxxxxx")
	}

	count := len(modelTipSetList)
	if count == 0 {
		return nil
	}

	if modelTipSetList[0].ParentTipSetID != tsID {
		log.Warnf("getHeadChain failed, maybe head changed %v %v %v", height, tsID, modelTipSetList[0].ParentTipSetID)
		return nil
	}

	headChainList := make([]*dao.HeadChainInfo, count)
	for i, v := range modelTipSetList {
		ts := &dao.HeadChainInfo{
			Height:       v.Height,
			ParentHeight: v.ParentHeight,
			ID:           v.ID,
			ParentID:     v.ParentTipSetID,
		}
		headChainList[i] = ts
	}

	return headChainList
}

func (c *CacheChain) getForkPoint(curChainHead *dao.HeadChainInfo, oldHeadChainList []*dao.HeadChainInfo) (int64, uint64) {
	lastHeight := int64(0)
	lastTsID := uint64(0)
	count := len(oldHeadChainList)
	if count > 0 {
		lastHeight = oldHeadChainList[count-1].ParentHeight
		lastTsID = oldHeadChainList[count-1].ParentID
	} else {
		lastHeight = curChainHead.Height
		lastTsID = curChainHead.ID
	}

	return lastHeight, lastTsID
}

func (c *CacheChain) start() {

	status_query := c.rds.Get(c.ctx, dao.BuildChainHeadKey())
	res, err := status_query.Result()
	if err != nil {
		panic(err)
	}

	var info dao.HeadChainInfo

	if err := json.Unmarshal([]byte(res), &info); err != nil {
		panic(err)
	}

	curChainHead := &info

	log.Infow("read from cache chain head", "height", curChainHead.Height, "parent_height", curChainHead.ParentHeight, "id", curChainHead.ID, "parent_id", curChainHead.ParentID)

	go func() {

		inProcess := atomic.NewBool(false)

		timer := time.NewTicker(3 * time.Second)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:

				if inProcess.Load() {
					continue
				}

				inProcess.Store(true)

				go func() {
					defer func() {
						inProcess.Store(false)
					}()

					curChainHead = c.cache(curChainHead)
				}()
			}
		}
	}()
}

func (c *CacheChain) cache(curChainHead *dao.HeadChainInfo) *dao.HeadChainInfo {
	newChainHead := c.getChainHead()

	if curChainHead.ID == newChainHead.ID {
		return curChainHead
	}

	// below is head change

	// get old headchain list
	oldHeadChainList := c.getBranchFromHeadToHeadChain(curChainHead.ID)

	// get forkpoint tipset
	lastHeight, lastTsID := c.getForkPoint(curChainHead, oldHeadChainList)

	pipe := c.rds.TxPipeline()
	defer pipe.Close()

	// NOTE: only ensure they are different branches with same fork point, because old branch and new branch may not consistent with database.

	revertAffectKeyCount := 0
	// rollback old branch, from high to low
	{
		set, addTo, _ := dao.GetBatchHeightChangedData(c.ctx, c.db, oldHeadChainList)
		for k, _ := range set {
			pipe.Del(c.ctx, k)
			revertAffectKeyCount++
		}
		for _, info := range addTo {
			pipe.RPop(c.ctx, info.Key)
			revertAffectKeyCount++
		}
	}

	if len(oldHeadChainList) > 0 {
		for _, v := range oldHeadChainList {
			log.Debugf("revert detail height=%v tsID=%v parentHeigh=%v parentTsID=%v", v.Height, v.ID, v.ParentHeight, v.ParentID)
		}
		log.Infof("cache revert summarize(tipset_count=%v affect=%v) height=%v tsID=%v ==>> height=%v tsID=%v", len(oldHeadChainList), revertAffectKeyCount, curChainHead.Height, curChainHead.ID, lastHeight, lastTsID)
	}

	head := curChainHead

	// grow new branch, from low to high
	for {
		growAffectKeyCount := 0

		newHeadChainList := c.getHeadChain(lastHeight, lastTsID)
		count := len(newHeadChainList)

		if count == 0 {
			break
		}

		tmpHead := newHeadChainList[count-1]

		set, addTo, _ := dao.GetBatchHeightChangedData(c.ctx, c.db, newHeadChainList)
		for k, v := range set {
			pipe.Set(c.ctx, k, v, dao.CacheTimeout)
			growAffectKeyCount++
		}
		for _, info := range addTo {
			pipe.RPush(c.ctx, info.Key, info.Value)
			growAffectKeyCount++
		}

		for _, v := range newHeadChainList {
			log.Debugf("grow detail parentHeigh=%v parentTsID=%v height=%v tsID=%v", v.ParentHeight, v.ParentID, v.Height, v.ID)
		}

		log.Infof("cache grow summarize(tipset_count=%v affect=%v) height=%v tsID=%v ==>> height=%v tsID=%v", count, growAffectKeyCount, lastHeight, lastTsID, tmpHead.Height, tmpHead.ID)

		chainNotify := set[dao.BuildTipSetDigestKey(tmpHead.Height)]

		byteChainHead, _ := json.Marshal(tmpHead)
		pipe.Set(c.ctx, dao.BuildChainHeadKey(), string(byteChainHead), 0)
		pipe.Publish(c.ctx, dao.BuildChainNotifyKey(), chainNotify)

		_, err := pipe.Exec(c.ctx)
		if err != nil {
			pipe.Discard()
			log.Warnf("write cache failed:%v", err)
			break
		}

		head = tmpHead
		lastTsID = head.ID
		lastHeight = head.Height

		lastHeight2 := lastHeight
		go func() {
			c.cacheChainHeadNotify <- CacheChainHead{height: lastHeight2}
		}()
	}

	return head
}
