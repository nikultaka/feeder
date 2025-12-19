package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
	mu     sync.RWMutex
}

var redisLocalClient RedisClient

func InitRedis() {
	redisLocalClient = RedisClient{}
	redisLocalClient.client = redis.NewClient(&redis.Options{
		Addr:     Env.RedisAddress,
		Password: Env.RedisPassword,
		DB:       Env.RedisDB,
	})
}

func (rc *RedisClient) getValueFromRedis(key string) string {
	ctx := context.Background()

	rc.mu.RLock()
	defer rc.mu.RUnlock()
	val, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		// Logger.Error().Msg(fmt.Sprintf("Redis Key doesn't exist %s", key))
	} else if err != nil {
		Logger.Error().Err(err).Msg(fmt.Sprintf("Failed to get value from redis for key %s", key))
	}
	return val
}

func (rc *RedisClient) saveToRedis(key string, data interface{}, isJSON bool) {
	var value interface{}
	if isJSON {
		jsonFinalData, err := json.Marshal(data)
		if err != nil {
			Logger.Error().Err(err).Msg(fmt.Sprintf("Failed to marshal JSON for key %s", key))
			return
		}
		value = string(jsonFinalData)
	} else {
		value = data
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()
	if err := rc.client.Set(context.Background(), key, value, 0).Err(); err != nil {
		Logger.Error().Err(err).Msg(fmt.Sprintf("Failed to set sheet data in redis for key %s", key))
	}
}

func (rc *RedisClient) sendPublishMessageToChannel(channel string, key string, data interface{}) {
	jsonFinalData, err := json.Marshal(data)
	if err != nil {
		Logger.Error().Err(err).Msg(fmt.Sprintf("[sendPublishMessageToChannel] [%s] [%s] JSON Marshal Error", channel, key))
		return
	}

	var formatMsg string
	if key == "" {
		formatMsg = string(jsonFinalData)
	} else {
		formatMsg = fmt.Sprintf("%s#%s", key, string(jsonFinalData))
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.client.Publish(context.Background(), channel, formatMsg)
}

func PublishSymbolSheetData() {
	if err := LoadClientSheetDetailsConfig(Env.GoogleSpreadsheetID, Env.ClientDetailsSheetID); err != nil {
		Logger.Error().Err(err).Msg("[PublishSymbolSheetData] [loadClientSheetDetailsConfig] LoadClientSheetDetailsConfig")
		return
	}

	if err := LoadClientAPIDetailsConfig(Env.GoogleSpreadsheetID, Env.ClientAPISheetID); err != nil {
		Logger.Error().Err(err).Msg("[PublishSymbolSheetData] [loadClientSheetDetailsConfig] loadClientAPIDetailsConfig")
		return
	}

	fmt.Println("ClientAPIDetailsConfigData ========, ", ClientAPIDetailsConfigData)
	redisLocalClient.sendPublishMessageToChannel("clients", "client_sheet_details", ClientSheetDetailsConfigData)
	redisLocalClient.sendPublishMessageToChannel("clients", "client_api_details", ClientAPIDetailsConfigData)
	Logger.Info().Msg("[PublishSymbolSheetData] update messages sent")
}
