package worker

import (
	"fmt"
	"sync"
	"time"
)

type TickDataStorageStruct struct {
	data map[string]WSResponse
	mu   sync.Mutex
}

var TickDataStorage *TickDataStorageStruct

func InitWorkers() {
	ZerodhaWSWorker = &WSWorker{IsRunning: false, Mutex: sync.Mutex{}}
	DGPlusWSWorker = &WSWorker{IsRunning: false, Mutex: sync.Mutex{}}
	SwapDataFeedWSWorker = &WSWorker{IsRunning: false, Mutex: sync.Mutex{}}
	FeederDataFeedWSWorker = &WSWorker{IsRunning: false, Mutex: sync.Mutex{}}
	DiwjDataFDWWSWorker = &WSWorker{IsRunning: false, Mutex: sync.Mutex{}}
	TickDataStorage = &TickDataStorageStruct{data: map[string]WSResponse{}}
}

func AddDataToTickDataStorage(key string, data WSResponse) {
	TickDataStorage.mu.Lock()
	defer TickDataStorage.mu.Unlock()
	TickDataStorage.data[key] = data
}

func SendTickDataWorker(retryCount int) {
	defer func() {
		if r := recover(); r != nil {
			Logger.Error().Msg(fmt.Sprintf("SendTickDataWorker goroutine paniqued: %v", r))
			if retryCount < Env.MaxRetries {
				retryCount++
				Logger.Error().Msg(fmt.Sprintf("Retrying SendTickDataWorker (%d/%d) ", retryCount, Env.MaxRetries))
				time.Sleep(time.Second)
				go SendTickDataWorker(retryCount)
			} else {
				Logger.Error().Msg("Max retries reached. Gracefully shutting down SendTickDataWorker.")
			}
		}
	}()

	for {
		time.Sleep(time.Duration(Env.TickDataSleepInMS) * time.Millisecond)
		TickDataStorage.mu.Lock()
		if len(TickDataStorage.data) != 0 {
			redisLocalClient.sendPublishMessageToChannel("ticker", "", TickDataStorage.data)
			TickDataStorage.data = map[string]WSResponse{}
		}
		TickDataStorage.mu.Unlock()
	}
}
