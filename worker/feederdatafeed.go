package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
	"trading-worker-api/models"

	"github.com/gorilla/websocket"
)

var feederSymbols []string
var feederDgMap sync.Map

var (
	feederWSResponse           sync.Map
	feederSymbolMap            sync.Map
	FeederWorkerSuccessChannel = make(chan bool)
	FeederWSWorker             struct {
		IsRunning bool
		Mutex     sync.Mutex
	}
	feedCtx    context.Context
	feedCancel context.CancelFunc
)

type Depth struct {
	Buy  []interface{} `json:"buy"`
	Sell []interface{} `json:"sell"`
}

// MarketData represents the structure of the JSON data
type FeederMarketData struct {
	Ask              float64     `json:"ask"`
	Bid              float64     `json:"bid"`
	Ch               float64     `json:"ch"`
	Chp              float64     `json:"chp"`
	Close            float64     `json:"close"`
	Depth            Depth       `json:"depth"`
	Exchange         string      `json:"exchange"`
	Expiry           string      `json:"expiry"`
	High             float64     `json:"high"`
	LTP              float64     `json:"ltp"`
	LC               float64     `json:"lc"`
	LS               float64     `json:"ls"`
	Low              float64     `json:"low"`
	LTT              interface{} `json:"ltt"`
	Open             float64     `json:"open"`
	Symbol           string      `json:"symbol"`
	SymbolMasterName string      `json:"name"`
	Volume           float64     `json:"volume"`
	Tbt              float64     `json:"tbq"`
	Tsq              float64     `json:"tsq"`
	UC               float64     `json:"uc"`
}

type FeederSymbol struct {
	Description      string `json:"description"`
	Digits           int    `json:"digits"`
	Exchange         string `json:"exchange"`
	Expiry           string `json:"expiry"`
	LotSize          int    `json:"lot_size"`
	SymbolMasterName string `json:"symbol_master_name"`
	SymbolName       string `json:"symbol_name"`
}

func StartFeederWorker() {
	FeederWSWorker.Mutex.Lock()
	if FeederWSWorker.IsRunning {
		FeederWSWorker.Mutex.Unlock()
		log.Println("Feeder Worker is already running.")
		return
	}
	FeederWSWorker.IsRunning = true
	FeederWSWorker.Mutex.Unlock()

	PublishSymbolSheetData()

	// TEST: Add a second BTCUSD entry with different expiry to test multiple expiry dates
	testConfig := feederConfig{
		InstrumentToken:     14536643,
		ExchangeToken:       "985254",
		TradingSymbol:       "BTCUSD",
		TradingName:         "BTCUSD",
		SubscribeSymbolName: "BTCUSD_MIX",
		Exchange:            "CRYPTO",
		Expiry:              "2025-12-31",
	}
	FeederConfigData["14536643_TEST"] = testConfig
	log.Println("TEST: Added second BTCUSD entry with expiry 2025-12-31 for testing")

	// Add USDSGD configuration
	usdsgdConfig := feederConfig{
		InstrumentToken:     99999999,
		ExchangeToken:       "999999",
		TradingSymbol:       "USDSGD",
		TradingName:         "USDSGD",
		SubscribeSymbolName: "USDSGD",
		Exchange:            "FOREX",
		Expiry:              "NO EXPIRY",
	}
	FeederConfigData["99999999"] = usdsgdConfig
	log.Println("Added USDSGD configuration to feeder worker")

	feederSymbols = []string{}
	symbolSet := make(map[string]bool) // Track unique symbols for subscription
	for _, row := range FeederConfigData {
		// fmt.Println("row", row)
		fmt.Println("row", row)
		log.Println("row Feeder Worker.", row)

		// Use composite key: SubscribeSymbolName + Expiry to handle multiple expiries
		compositeKey := row.SubscribeSymbolName + "|" + row.Expiry
		feederDgMap.Store(compositeKey, row)

		// Only add unique subscribe symbols to avoid duplicates in subscription
		if !symbolSet[row.SubscribeSymbolName] {
			feederSymbols = append(feederSymbols, row.SubscribeSymbolName)
			symbolSet[row.SubscribeSymbolName] = true
		}

		log.Printf("Stored feeder config with key: %s (Symbol: %s, Expiry: %s)", compositeKey, row.SubscribeSymbolName, row.Expiry)
	}

	ctx, cancel = context.WithCancel(context.Background())
	log.Println("Starting Feeder Worker.")
	go connectToFeeder()
	log.Println("Feeder Worker started.")
}

func StopFeederWorker() {
	FeederWSWorker.Mutex.Lock()
	if !FeederWSWorker.IsRunning {
		FeederWSWorker.Mutex.Unlock()
		return
	}
	FeederWSWorker.IsRunning = false
	FeederWSWorker.Mutex.Unlock()

	cancel()
	log.Println("Feeder Worker stopped.")
}

func connectToFeeder() {
	log.Println("Connecting to Feeder WebSocket.")
	for FeederWSWorker.IsRunning {
		// Create header and add token
		headers := http.Header{}
		headers.Add("x-api-key", Env.FEEDER_WS_TOKEN)
		// You can add more headers if needed:
		// headers.Add("Custom-Header", "Value")

		conn, _, err := websocket.DefaultDialer.Dial(Env.FEEDER_WS_URL_N, headers)
		log.Println("Connecting to Feeder WebSocket.")
		// conn, _, err := websocket.DefaultDialer.Dial(Env.FEEDER_WS_URL_N, nil)
		if err != nil {
			log.Printf("Failed to connect to Feeder WebSocket: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("Connected to Feeder WebSocket.")

		if err := subscribeToFeeder(conn); err != nil {
			log.Printf("Subscription failed: %v", err)
			conn.Close()
			continue
		}

		readFeederMessages(ctx, conn)
		conn.Close()
	}
}

func subscribeToFeeder(conn *websocket.Conn) error {
	log.Println("Subscribing to Feeder symbols:", feederSymbols)
	payload := map[string]interface{}{
		"event":   "subscribe",
		"symbols": feederSymbols, //[]string{"BTCUSD_MIX"}, //feederSymbols, // Example symbols //["EURUSD", "USDCHF"]
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription payload: %v", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("failed to send subscription payload: %v", err)
	}

	log.Println("Subscription message sent.")
	return nil
}

func readFeederMessages(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping Feeder read loop.")
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Error reading Feeder message: %v", err)
				return
			}

			// log.Printf("Received Feeder message: %s", message)

			// Process the received message
			go func(m []byte) {
				data, err := handleFeederMessage(m)
				if err != nil {
					log.Printf("Failed to handle Feeder message: %v", err)
					return
				}

				processFeederMarketData(data)
			}(message)
		}
	}
}

// handleFeederMessage parses the JSON message and returns a MarketData instance.
func handleFeederMessage(message []byte) (FeederMarketData, error) {
	var data FeederMarketData
	err := json.Unmarshal(message, &data)
	if err != nil {
		return FeederMarketData{}, err
	}
	return data, nil
}
func processFeederMarketData(data FeederMarketData) {
	// log.Printf("Processing FeederMarketData: %+v", data)

	updatedJson, errM := json.Marshal(data)
	if errM != nil {
		fmt.Println("getting error for marshalling")
	}
	//if data.Symbol == "BTCUSD" || data.SymbolMasterName == "BTCUSD_MIX" {
	fmt.Println("json feeder data : ", string(updatedJson))
	//}

	log.Printf("Processing FeederMarketData: Symbol=%s, Name=%s, Expiry=%s", data.Symbol, data.SymbolMasterName, data.Expiry)

	// Use composite key to lookup: SymbolMasterName + Expiry
	compositeKey := data.SymbolMasterName + "|" + data.Expiry
	value, _ := feederDgMap.Load(compositeKey)

	// If not found and expiry is "NO EXPIRY" or empty, try to find any matching symbol
	if value == nil && (data.Expiry == "NO EXPIRY" || data.Expiry == "") {
		log.Printf("Expiry not provided, searching for any config matching symbol: %s", data.SymbolMasterName)
		// Iterate through all stored configs to find a match by symbol name
		var foundConfig feederConfig
		found := false
		feederDgMap.Range(func(key, val interface{}) bool {
			config := val.(feederConfig)
			if config.SubscribeSymbolName == data.SymbolMasterName {
				foundConfig = config
				found = true
				log.Printf("Found config for %s with expiry %s", data.SymbolMasterName, config.Expiry)
				return false // Stop iteration
			}
			return true // Continue iteration
		})
		if found {
			value = foundConfig
		}
	}

	if value == nil {
		log.Printf("Symbol not found in Feeder config (looked up by key='%s'), skipping processing.", compositeKey)
		return
	}
	feederData := value.(feederConfig)
	if !FeederWSWorker.IsRunning || isExpired(feederData.Expiry) {
		log.Println("Feeder Worker stopped or data expired, skipping processing.")
		return
	}
	var lttStr string

	switch v := data.LTT.(type) {
	case string:
		lttStr = v
	case float64: // JSON numbers are unmarshaled as float64 by default
		lttStr = strconv.FormatFloat(v, 'f', -1, 64)
	default:
		// log.Println("Unexpected type for ltt:", v)
		lttStr = ""
	}
	wsResponse := WSResponse{
		Bid:    data.Bid,
		Ask:    data.Ask,
		CH:     data.Ch,
		CHP:    data.Chp,
		LTP:    data.LTP,
		LTT:    lttStr,
		UC:     data.UC,
		LC:     data.LC,
		Open:   data.Open,
		High:   data.High,
		Low:    data.Low,
		Close:  data.Close,
		Volume: data.Volume,
		OI:     0,
		TBQ:    data.Tbt,
		TSQ:    data.Tsq,
		Symbol: feederData.TradingSymbol,
		Depth:  data.Depth,
	}
	// log.Printf("Processing wsResponse: %+v", wsResponse)
	// log.Println("feederData.TradingSymbol:", feederData.TradingSymbol)
	dgPlusWSResponse.Store(feederData.TradingSymbol, wsResponse)
	//AddDataToTickDataStorage(data.Symbol, wsResponse)

	fmt.Println(wsResponse)
	AddDataToTickDataStorage(feederData.TradingSymbol, wsResponse)

	tick := models.Tick{
		Symbol:     feederData.TradingSymbol,
		Ask:        data.Ask,
		Bid:        data.Bid,
		Ltp:        data.LTP,
		Open:       data.Open,
		High:       data.High,
		Low:        data.Low,
		Close:      data.Close,
		Volume:     data.Volume,
		Timestamp:  time.Now(),
		ExpiryDate: data.Expiry,
		Exchange:   feederData.Exchange,
	}

	log.Printf("Saving tick - Symbol: %s, Exchange: %s, Expiry: %s", tick.Symbol, tick.Exchange, tick.ExpiryDate)

	err := tick.Save()
	if err != nil {
		log.Println("Failed to save tick:", err)
	} else {
		log.Printf("Tick saved successfully for symbol: %s with expiry_date: %s", tick.Symbol, tick.ExpiryDate)
	}
}
