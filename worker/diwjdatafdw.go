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

	"github.com/gorilla/websocket"
)

var diwjDataFDWSymbols []string
var diwjDataFDWDgMap sync.Map

var (
	diwjDataFDWWSResponse           sync.Map
	diwjDataFDWSymbolMap            sync.Map
	DiwjDataFDWWorkerSuccessChannel = make(chan bool)
	diwjFeedCtx                     context.Context
	diwjFeedCancel                  context.CancelFunc
)

type DiwjDepth struct {
	Buy  []interface{} `json:"buy"`
	Sell []interface{} `json:"sell"`
}

// MarketData represents the structure of the JSON data
type DiwjDataFDWMarketDatum struct {
	Bid      float64     `json:"bid"`
	Ask      float64     `json:"ask"`
	Ch       float64     `json:"ch"`
	Chp      float64     `json:"chp"`
	Ltp      float64     `json:"ltp"`
	Ltt      interface{} `json:"ltt"` // or time.Time if you parse it separately
	Uc       *float64    `json:"uc"`  // nullable, so pointer
	Lc       *float64    `json:"lc"`  // nullable, so pointer
	Open     float64     `json:"open"`
	High     float64     `json:"high"`
	Low      float64     `json:"low"`
	Close    float64     `json:"close"`
	Volume   int64       `json:"volume"`
	Oi       int64       `json:"oi"`
	Tbq      int64       `json:"tbq"`
	Tsq      int64       `json:"tsq"`
	Symbol   string      `json:"symbol"`
	Depth    interface{} `json:"depth"` // null in JSON, so interface{}
	Expiry   string      `json:"expiry"`
	Exchange string      `json:"exchange"`
	Ts       float64     `json:"ts"`
	Ls       float64     `json:"ls"`
}

type DiwjDataFDWMarketData struct {
	Status  bool                     `json:"status"`
	Message string                   `json:"message"`
	Data    []DiwjDataFDWMarketDatum `json:"data"`
}

type DiwjDataFDWSymbol struct {
	Description      string `json:"description"`
	Digits           int    `json:"digits"`
	Exchange         string `json:"exchange"`
	Expiry           string `json:"expiry"`
	LotSize          int    `json:"lot_size"`
	SymbolMasterName string `json:"symbol_master_name"`
	SymbolName       string `json:"symbol_name"`
}

func StartDiwjDataFDWWorker() {
	DiwjDataFDWWSWorker.Mutex.Lock()
	if DiwjDataFDWWSWorker.IsRunning {
		DiwjDataFDWWSWorker.Mutex.Unlock()
		log.Println("DiwjDataFDW Worker is already running.")
		return
	}
	DiwjDataFDWWSWorker.IsRunning = true
	DiwjDataFDWWSWorker.Mutex.Unlock()

	PublishSymbolSheetData()

	diwjDataFDWSymbols = []string{}
	for _, row := range DiwjDataFDWConfigData {
		// fmt.Println("row", row)
		fmt.Println("row", row)
		log.Println("row DiwjDataFDW Worker.", row)
		diwjDataFDWDgMap.Store(row.SubscribeSymbolName, row)
		diwjDataFDWSymbols = append(diwjDataFDWSymbols, row.SubscribeSymbolName)
	}

	ctx, cancel = context.WithCancel(context.Background())
	log.Println("Starting DiwjDataFDW Worker.")
	go connectToDiwjDataFDW()
	log.Println("DiwjDataFDW Worker started.")
}

func StopDiwjDataFDWWorker() {
	DiwjDataFDWWSWorker.Mutex.Lock()
	if !DiwjDataFDWWSWorker.IsRunning {
		DiwjDataFDWWSWorker.Mutex.Unlock()
		return
	}
	DiwjDataFDWWSWorker.IsRunning = false
	DiwjDataFDWWSWorker.Mutex.Unlock()

	cancel()
	log.Println("DiwjDataFDW Worker stopped.")
}

func connectToDiwjDataFDW() {
	log.Println("Connecting to DiwjDataFDW WebSocket.")
	for DiwjDataFDWWSWorker.IsRunning {
		log.Println("Connecting to DiwjDataFDW WebSocket.")

		// Create header and add token
		headers := http.Header{}
		headers.Add("Authorization", "Bearer "+Env.DIWJ_FEEDER_WS_TOKEN)
		// You can add more headers if needed:
		// headers.Add("Custom-Header", "Value")

		conn, _, err := websocket.DefaultDialer.Dial(Env.DIWJ_FEEDER_WS_URL_N, headers)
		if err != nil {
			log.Printf("Failed to connect to DiwjDataFDW WebSocket: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("Connected to DiwjDataFDW WebSocket.")

		if err := subscribeToDiwjDataFDW(conn); err != nil {
			log.Printf("Subscription failed: %v", err)
			conn.Close()
			continue
		}

		readDiwjDataFDWMessages(ctx, conn)
		conn.Close()
	}
}

func subscribeToDiwjDataFDW(conn *websocket.Conn) error {
	log.Println("Subscribing to DiwjDataFDW symbols:", diwjDataFDWSymbols)
	payload := map[string]interface{}{
		"symbols": diwjDataFDWSymbols, // Example symbols
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

func readDiwjDataFDWMessages(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping DiwjDataFDW read loop.")
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Error reading DiwjDataFDW message: %v", err)
				return
			}

			// log.Printf("Received DiwjDataFDW message: %s", message)

			// Process the received message
			go func(m []byte) {
				data, err := handleDiwjDataFDWMessage(m)
				if err != nil {
					log.Printf("Failed to handle DiwjDataFDW message: %v", err)
					return
				}
				if len(data.Data) > 0 {
					processDiwjDataFDWMarketData(data.Data[0])
				}
				// processDiwjDataFDWMarketData(data)
			}(message)
		}
	}
}

// handleDiwjDataFDWMessage parses the JSON message and returns a MarketData instance.
func handleDiwjDataFDWMessage(message []byte) (DiwjDataFDWMarketData, error) {
	var data DiwjDataFDWMarketData
	err := json.Unmarshal(message, &data)
	if err != nil {
		return DiwjDataFDWMarketData{}, err
	}
	return data, nil
}
func processDiwjDataFDWMarketData(data DiwjDataFDWMarketDatum) {
	// log.Printf("Processing DiwjDataFDWMarketData: %+v", data)
	value, _ := diwjDataFDWDgMap.Load(data.Symbol)
	if value == nil {
		log.Println("Symbol not found in DiwjDataFDW config, skipping processing.")
		return
	}
	diwjDataFDWData := value.(diwjDataFDWConfig)
	// if !DiwjDataFDWWSWorker.IsRunning || isExpired(diwjDataFDWData.Expiry) {
	if !DiwjDataFDWWSWorker.IsRunning {
		log.Println("DiwjDataFDW Worker stopped or data expired, skipping processing.")
		return
	}
	var lttStr string
	switch v := data.Ltt.(type) {
	case string:
		lttStr = v
	case float64: // JSON numbers are unmarshaled as float64 by default
		lttStr = strconv.FormatFloat(v, 'f', -1, 64)
	default:
		// log.Println("Unexpected type for ltt:", v)
		lttStr = ""
	}
	// log.Printf("Processing DiwjDataFDWMarketData: %+v", diwjDataFDWData)
	wsResponse := WSResponse{
		Bid:    data.Bid,
		Ask:    data.Ask,
		CH:     data.Ch,
		CHP:    data.Chp,
		LTP:    data.Ltp,
		LTT:    lttStr,
		UC:     data.Uc,
		LC:     data.Lc,
		Open:   data.Open,
		High:   data.High,
		Low:    data.Low,
		Close:  data.Close,
		Volume: float64(data.Volume),
		OI:     0,
		TBQ:    float64(data.Tbq),
		TSQ:    float64(data.Tsq),
		Symbol: diwjDataFDWData.TradingSymbol,
		Depth:  data.Depth,
	}
	// log.Printf("Processing wsResponse: %+v", wsResponse)
	// log.Println("diwjDataFDWData.TradingSymbol:", diwjDataFDWData.TradingSymbol)
	dgPlusWSResponse.Store(diwjDataFDWData.TradingSymbol, wsResponse)
	//AddDataToTickDataStorage(data.Symbol, wsResponse)
	AddDataToTickDataStorage(diwjDataFDWData.TradingSymbol, wsResponse)
}
