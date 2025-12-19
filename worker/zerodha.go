package worker

import (
	"context"
	"fmt"
	"time"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	kitemodels "github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

var ticker *kiteticker.Ticker
var instrumentTokenList = []uint32{}

const zerodhaAccessTokenRedisKey string = "ZerodhaAccessToken"

func SetZerodhaAccessTokenInLocalRedis(token string) {
	redisLocalClient.saveToRedis(zerodhaAccessTokenRedisKey, token, false)
}

func GetZerodhaAccessTokenFromLocalRedis() string {
	return redisLocalClient.getValueFromRedis(zerodhaAccessTokenRedisKey)
}

// Triggered when any error is raised
func onError(err error) {
	Logger.Error().Err(err).Msg("[zerodha] onError called")
}

// Triggered when websocket connection is closed
func onClose(code int, reason string) {
	Logger.Error().Msg(fmt.Sprintf("[zerodha] [onClose] code=%d, reason=%s", code, reason))
}

// Triggered when connection is established and ready to send and accept data
func onConnect() {
	Logger.Info().Msg("[zerodha] [onConnect] Connected to zerodha websocket")
	err := ticker.Subscribe(instrumentTokenList)
	if err != nil {
		Logger.Error().Err(err).Msg("[zerodha] [onConnect] Failed to subscribe to token list")
		return
	}
	// Set subscription mode for the subscribed token
	// Default mode is Quote
	err = ticker.SetMode(kiteticker.ModeFull, instrumentTokenList)
	if err != nil {
		Logger.Error().Err(err).Msg("[zerodha] [onConnect] Failed to set ticker mode")
		return
	}

}

// Triggered when tick is recevived
func onTick(tick kitemodels.Tick) {
	zerodhaConfigData, ok := ZerodhaConfigData[tick.InstrumentToken]
	if !ok {
		return
	}
	tickData := WSResponse{}
	tickData.Bid = roundFloat(tick.Depth.Buy[0].Price)
	tickData.Ask = roundFloat(tick.Depth.Sell[0].Price)

	if tickData.Bid == 0 || tickData.Ask == 0 {
		return
	}

	//change_percent = ((LTP - closePrice) * 100 / closePrice
	CHP := (tick.LastPrice - tick.OHLC.Close) * 100 / tick.OHLC.Close

	tickData.CH = roundFloat(tick.NetChange)
	tickData.CHP = roundFloat(CHP)
	tickData.LTP = roundFloat(tick.LastPrice)
	tickData.Open = roundFloat(tick.OHLC.Open)
	tickData.High = roundFloat(tick.OHLC.High)
	tickData.Low = roundFloat(tick.OHLC.Low)
	tickData.Close = roundFloat(tick.OHLC.Close)
	tickData.Volume = float64(tick.VolumeTraded)
	tickData.OI = roundFloat(float64(tick.OI))
	tickData.TBQ = roundFloat(float64(tick.TotalBuyQuantity))
	tickData.TSQ = roundFloat(float64(tick.TotalSellQuantity))
	tickData.Depth = tick.Depth
	tickData.Symbol = zerodhaConfigData.TradingSymbol

	tickData.LTT = GetUTCISODatetime()
	if zerodhaConfigData.Exchange == "NFUT" || zerodhaConfigData.Exchange == "MCX" {
		if zerodhaConfigData.LowerCircuitLevel1 != 0 && zerodhaConfigData.UpperCircuitLevel1 != 0 {
			lowerCircuit := tick.OHLC.Close * (1 - float64(zerodhaConfigData.LowerCircuitLevel1)/100)
			upperCircuit := tick.OHLC.Close * (1 + float64(zerodhaConfigData.UpperCircuitLevel1)/100)
			if (tick.LastPrice < lowerCircuit || tick.LastPrice > upperCircuit) && (zerodhaConfigData.LowerCircuitLevel2 != 0 && zerodhaConfigData.UpperCircuitLevel2 != 0) {
				lowerCircuit = tick.OHLC.Close * (1 - float64(zerodhaConfigData.LowerCircuitLevel2)/100)
				upperCircuit = tick.OHLC.Close * (1 + float64(zerodhaConfigData.UpperCircuitLevel2)/100)
				if (tick.LastPrice < lowerCircuit || tick.LastPrice > upperCircuit) && (zerodhaConfigData.LowerCircuitLevel3 != 0 && zerodhaConfigData.UpperCircuitLevel3 != 0) {
					lowerCircuit = tick.OHLC.Close * (1 - float64(zerodhaConfigData.LowerCircuitLevel3)/100)
					upperCircuit = tick.OHLC.Close * (1 + float64(zerodhaConfigData.UpperCircuitLevel3)/100)
				}
			}
			tickData.LC = roundFloat(lowerCircuit)
			tickData.UC = roundFloat(upperCircuit)
		}
	}

	AddDataToTickDataStorage(zerodhaConfigData.TradingSymbol, tickData)
}

// Triggered when reconnection is attempted which is enabled by default
func onReconnect(attempt int, delay time.Duration) {
	Logger.Error().Msg(fmt.Sprintf("[zerodha] [onReconnect] Reconnect attempt %d in %fs", attempt, delay.Seconds()))
}

// Triggered when maximum number of reconnect attempt is made and the program is terminated
func onNoReconnect(attempt int) {
	ZerodhaWSWorker.Mutex.Lock()
	defer ZerodhaWSWorker.Mutex.Unlock()
	ZerodhaWSWorker.IsRunning = false
	SetZerodhaAccessTokenInLocalRedis("")
	Logger.Error().Msg(fmt.Sprintf("[zerodha] [onNoReconnect] Maximum no of reconnect attempt reached: %d", attempt))
}

func UpdateZerodhaAccessToken(apiKey string, apiSecret string, requestToken string) error {
	kc := kiteconnect.New(apiKey)

	data, err := kc.GenerateSession(requestToken, apiSecret)
	if err != nil {
		SetZerodhaAccessTokenInLocalRedis("")
		Logger.Error().Err(err).Msg("[UpdateZerodhaAccessToken] Failed to GenerateSession")
		return err
	}

	SetZerodhaAccessTokenInLocalRedis(data.AccessToken)
	return nil
}

func zerodhaWSHandler() {
	Logger.Info().Msg("[ZerodhaWSHandler] Connecting to zerodha websocket ...")

	// Create new Kite ticker instance
	ticker = kiteticker.New(Env.ZerodhaAPIKey, GetZerodhaAccessTokenFromLocalRedis())

	// Assign callbacks
	ticker.SetAutoReconnect(true)
	ticker.SetReconnectMaxRetries(4)
	ticker.OnError(onError)
	ticker.OnClose(onClose)
	ticker.OnConnect(onConnect)
	ticker.OnReconnect(onReconnect)
	ticker.OnNoReconnect(onNoReconnect)
	ticker.OnTick(onTick)
	// ticker.OnOrderUpdate(onOrderUpdate)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	// Start the connection
	go ticker.ServeWithContext(ctx)

	for {
		if !ZerodhaWSWorker.IsRunning {
			time.Sleep(1 * time.Second)
			Logger.Info().Msg("[ZerodhaWSHandler] zerodha websocket stopped")
			cancel()
			return
		}
	}
}

func StartZerodhaWorker() {
	ZerodhaWSWorker.Mutex.Lock()
	defer ZerodhaWSWorker.Mutex.Unlock()
	ZerodhaWSWorker.IsRunning = true

	PublishSymbolSheetData()
	instrumentTokenList = []uint32{}
	for _, row := range ZerodhaConfigData {
		instrumentTokenList = append(instrumentTokenList, row.InstrumentToken)
	}

	Logger.Info().Msg(fmt.Sprintf("[StartZerodhaWorker] Total symbols: %d", len(instrumentTokenList)))
	go zerodhaWSHandler()
}

func StopZerodhaWorker() {
	ZerodhaWSWorker.Mutex.Lock()
	defer ZerodhaWSWorker.Mutex.Unlock()
	ZerodhaWSWorker.IsRunning = false
	time.Sleep(1 * time.Second)
}
