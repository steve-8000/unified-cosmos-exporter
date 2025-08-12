package util

import (
	"fmt"
	"strconv"
	"time"
)

type BlockTimeCalculator struct {
	lastBlockTime    time.Time
	lastBlockHeight  int64
	blockTimeHistory []time.Duration
	maxHistorySize   int
}

func NewBlockTimeCalculator(maxHistorySize int) *BlockTimeCalculator {
	if maxHistorySize <= 0 {
		maxHistorySize = 100
	}
	return &BlockTimeCalculator{
		blockTimeHistory: make([]time.Duration, 0, maxHistorySize),
		maxHistorySize:   maxHistorySize,
	}
}

func (btc *BlockTimeCalculator) UpdateBlockTime(height int64, blockTime time.Time) {
	if btc.lastBlockHeight > 0 && height > btc.lastBlockHeight {
		timeDiff := blockTime.Sub(btc.lastBlockTime)
		btc.blockTimeHistory = append(btc.blockTimeHistory, timeDiff)
		
		if len(btc.blockTimeHistory) > btc.maxHistorySize {
			btc.blockTimeHistory = btc.blockTimeHistory[1:]
		}
	}
	
	btc.lastBlockTime = blockTime
	btc.lastBlockHeight = height
}

func (btc *BlockTimeCalculator) GetAverageBlockTime() time.Duration {
	if len(btc.blockTimeHistory) == 0 {
		return 0
	}
	
	var total time.Duration
	for _, duration := range btc.blockTimeHistory {
		total += duration
	}
	
	return total / time.Duration(len(btc.blockTimeHistory))
}

func (btc *BlockTimeCalculator) GetLatestBlockTime() time.Duration {
	if len(btc.blockTimeHistory) == 0 {
		return 0
	}
	return btc.blockTimeHistory[len(btc.blockTimeHistory)-1]
}

func (btc *BlockTimeCalculator) GetBlockTimeStats() (avg, min, max time.Duration) {
	if len(btc.blockTimeHistory) == 0 {
		return 0, 0, 0
	}
	
	min = btc.blockTimeHistory[0]
	max = btc.blockTimeHistory[0]
	var total time.Duration
	
	for _, duration := range btc.blockTimeHistory {
		total += duration
		if duration < min {
			min = duration
		}
		if duration > max {
			max = duration
		}
	}
	
	avg = total / time.Duration(len(btc.blockTimeHistory))
	return avg, min, max
}

func (btc *BlockTimeCalculator) EstimateBlocksInDuration(duration time.Duration) int64 {
	avgBlockTime := btc.GetAverageBlockTime()
	if avgBlockTime == 0 {
		return 0
	}
	
	return int64(duration / avgBlockTime)
}

func (btc *BlockTimeCalculator) EstimateTimeForBlocks(blockCount int64) time.Duration {
	avgBlockTime := btc.GetAverageBlockTime()
	return avgBlockTime * time.Duration(blockCount)
}

func (btc *BlockTimeCalculator) IsBlockTimeStable() bool {
	if len(btc.blockTimeHistory) < 10 {
		return false
	}
	
	avg, min, max := btc.GetBlockTimeStats()
	if avg == 0 {
		return false
	}
	
	variance := float64(max-min) / float64(avg)
	return variance < 0.5
}

func (btc *BlockTimeCalculator) GetHistorySize() int {
	return len(btc.blockTimeHistory)
}

func (btc *BlockTimeCalculator) SetInitialBlockTime(blockTime time.Duration) {
	btc.blockTimeHistory = []time.Duration{blockTime}
	btc.lastBlockTime = time.Now()
	
	fmt.Printf("BlockTimeCalculator initialized with external block time: %v\n", blockTime)
}

func (btc *BlockTimeCalculator) Reset() {
	btc.blockTimeHistory = btc.blockTimeHistory[:0]
	btc.lastBlockTime = time.Time{}
	btc.lastBlockHeight = 0
}

func ParseBlockTime(blockTimeStr string) (time.Time, error) {
	blockTime, err := time.Parse(time.RFC3339, blockTimeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse block time: %w", err)
	}
	return blockTime, nil
}

func ParseBlockHeight(heightStr string) (int64, error) {
	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block height: %w", err)
	}
	return height, nil
}

func CalculateDowntimeThreshold(downtimeJailDuration float64, blockTime time.Duration) int {
	if downtimeJailDuration <= 0 || blockTime <= 0 {
		return 0
	}
	
	downtimeSeconds := downtimeJailDuration
	blockTimeSeconds := float64(blockTime.Seconds())
	
	if blockTimeSeconds <= 0 {
		return 0
	}
	
	threshold := int(downtimeSeconds / blockTimeSeconds)
	if threshold < 1 {
		threshold = 1
	}
	
	return threshold
}
