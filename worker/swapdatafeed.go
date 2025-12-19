package worker

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var swapSymbols []string
var swapDgMap sync.Map

type MarketData struct {
	Symbol      string  `json:"SYMBOL"`
	LTP         float64 `json:"LTP"`
	LTQ         int     `json:"LTQ"`
	Volume      int     `json:"VOLUME"`
	LastTrdTime int64   `json:"LastTrdTime"`
	BidQ        int     `json:"BidQ"`
	Bid         float64 `json:"Bid"`
	Ask         float64 `json:"Ask"`
	AskQ        int     `json:"AskQ"`
	Open        float64 `json:"Open"`
	High        float64 `json:"High"`
	Low         float64 `json:"Low"`
	PClose      float64 `json:"PClose"`
	OI          int     `json:"OI"`
	AvgPrice    float64 `json:"AvgPrice"`
	Exch        string  `json:"Exch"`
	Token       int     `json:"Token"`
}

const (
	swapDataFeedWSURL = "ws://b2.swapdatafeed.com:10680"
	loginID           = "wsapiforex2"
	loginPassword     = "027f966417577661b7cf3c1656dc4781"
)

var (
	interruptChan                 chan os.Signal
	SwapDataFeedWorkerSuccessChan = make(chan bool)
	done                          = make(chan bool)
	ctx                           context.Context
	cancel                        context.CancelFunc
)

type ConnPool struct {
	pool    chan *websocket.Conn
	maxSize int
}

// isExpired takes a date string in the format "YYYY-MM-DD" and checks if it is before the current date.
func isExpired(dateStr string) bool {
	// Define the layout matching the input date format.
	const layout = "2006-01-02" // This layout is Go's reference date format; must use these exact numbers.

	// Parse the input string to time.Time
	date, err := time.Parse(layout, dateStr)
	if err != nil {
		fmt.Println("time.Parse error =", err)
		return false // return error if the date string is not in the correct format
	}

	// Compare the parsed date with the current time
	// currentTime := time.Now()
	// Get today's date without time component
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	if date.Before(today) {
		return true // The date is expired
	}
	return false // The date is not expired
}

// min returns the smaller of x or y.
func min(x, y time.Duration) time.Duration {
	if x < y {
		return x
	}
	return y
}
func NewConnPool(maxSize int) *ConnPool {
	return &ConnPool{
		pool:    make(chan *websocket.Conn, maxSize),
		maxSize: maxSize,
	}
}

func (cp *ConnPool) ReturnConnection(conn *websocket.Conn) {
	select {
	case cp.pool <- conn:
		// connection returned to pool
	default:
		// pool is full, close the connection
		conn.Close()
	}
}

func (cp *ConnPool) Close() {
	close(cp.pool)
	for conn := range cp.pool {
		conn.Close()
	}
}

type Semaphore struct {
	channel  chan struct{}
	capacity int
}

func NewSemaphore(capacity int) *Semaphore {
	return &Semaphore{
		channel:  make(chan struct{}, capacity),
		capacity: capacity,
	}
}

func (s *Semaphore) Acquire() {
	s.channel <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.channel
}

func (s *Semaphore) CurrentCount() int {
	return len(s.channel)
}

func (cp *ConnPool) GetConnection() (*websocket.Conn, error) {
	for {
		select {
		case conn := <-cp.pool:
			if conn != nil && websocket.IsUnexpectedCloseError(conn.Close()) {
				// Check if the connection is still open
				return conn, nil
			}
			// The connection is closed, skip it
		default:
			// Create a new connection if the pool is not full
			if len(cp.pool) < cp.maxSize {
				conn, _, err := websocket.DefaultDialer.Dial(swapDataFeedWSURL, nil)
				if err != nil {
					return nil, err
				}
				return conn, nil
			}
			// If pool is temporarily empty, block until a connection is available
			conn := <-cp.pool
			if conn != nil && !websocket.IsUnexpectedCloseError(conn.Close()) {
				return conn, nil
			}
			// If the connection is closed, continue to try again
		}
	}
}

func connectAndListenWithPool(pool *ConnPool, sem *Semaphore) {
	retryDelay := 1 * time.Second         // Initial retry delay
	const maxRetryDelay = 1 * time.Minute // Maximum retry delay
	fmt.Println("SwapDataFeedWSWorker.IsRunning ==", SwapDataFeedWSWorker.IsRunning)
	for SwapDataFeedWSWorker.IsRunning {
		sem.Acquire()
		conn, err := pool.GetConnection()
		if err != nil {
			log.Printf("Failed to get a connection: %v", err)
			time.Sleep(retryDelay)
			retryDelay = min(2*retryDelay, maxRetryDelay) // Exponential backoff
			sem.Release()
			continue
		}
		retryDelay = 1 * time.Second // Reset retry delay on successful connection

		if err = authenticateAndSubscribe(conn); err != nil {
			log.Printf("Authentication or subscription failed: %v", err)
			conn.Close() // Ensure the connection is closed before returning it
			sem.Release()
			continue
		}

		readMessages(ctx, conn)
		pool.ReturnConnection(conn)
		sem.Release()
	}
}

func StartSwapDataFeedWorker() {
	SwapDataFeedWSWorker.Mutex.Lock()
	if SwapDataFeedWSWorker.IsRunning {
		SwapDataFeedWSWorker.Mutex.Unlock()
		log.Println("Attempt to start worker, but it's already running.")
		return // Prevent starting the worker twice
	}
	SwapDataFeedWSWorker.IsRunning = true
	SwapDataFeedWSWorker.Mutex.Unlock()

	ctx, cancel = context.WithCancel(context.Background())

	PublishSymbolSheetData()

	swapSymbols = []string{}
	for _, row := range DGPlusConfigData {
		//fmt.Println("row", row)
		swapDgMap.Store(row.TradingName, row)
		swapSymbols = append(swapSymbols, row.TradingName)
	}

	// Initialize the connection pool and semaphore
	maxConnections := 5 // Set this to the maximum number of connections you want to allow at one time
	connPool := NewConnPool(maxConnections)
	semaphore := NewSemaphore(maxConnections)

	// Start managing WebSocket connections with the pool and semaphore
	go connectAndListenWithPool(connPool, semaphore)

	// Set up cleanup logic for when the worker stops
	/*defer func() {
		SwapDataFeedWSWorker.Mutex.Lock()
		SwapDataFeedWSWorker.IsRunning = false
		SwapDataFeedWSWorker.Mutex.Unlock()
		//connPool.Close()
		log.Println("Worker has stopped.")
	}()*/
}

func authenticateAndSubscribe(c *websocket.Conn) error {
	if err := login(c); err != nil {
		return err
	}
	return subscribe(c)
}

func login(c *websocket.Conn) error {
	loginMsg := fmt.Sprintf("Login,%s,%s", loginID, loginPassword)
	if err := c.WriteMessage(websocket.TextMessage, []byte(loginMsg)); err != nil {
		return fmt.Errorf("login error: %w", err)
	}

	_, message, err := c.ReadMessage()
	if err != nil {
		return fmt.Errorf("login response error: %w", err)
	}
	if string(message) != "LOGIN,SUCCESS" {
		return fmt.Errorf("login failed: %s", string(message))
	}
	log.Println("Login successful")
	return nil
}

func subscribe(c *websocket.Conn) error {
	//symbol := "BTCUSD,NASDAQ"
	symbol := strings.Join(swapSymbols, ",")
	subscribeMsg := fmt.Sprintf("subscribe,%s", symbol)
	if err := c.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
		return fmt.Errorf("subscribe error: %w", err)
	}
	log.Printf("Subscribed to symbols: %s", symbol)
	return nil
}

func stopWebSocket(c *websocket.Conn) {
	log.Println("Sending close frame to WebSocket server...")
	if err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		log.Printf("Failed to send close message: %v", err)
	}
	if err := c.Close(); err != nil {
		log.Printf("Failed to close WebSocket connection: %v", err)
	}
	log.Println("WebSocket connection closed.")
}

func readMessages(ctx context.Context, c *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Context done, closing WebSocket...")
			stopWebSocket(c)
			return
		default:
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v, stopping read loop...", err)
				return // Optionally trigger a reconnect here
			}

			// Process the message
			go func(m []byte) {
				if err := handleMessage(m); err != nil {
					log.Printf("Handle message error: %v", err)
				}
			}(message)
		}
	}
}

func handleMessage(message []byte) error {
	reader := csv.NewReader(bytes.NewReader(message))
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Error reading CSV: %v", err)
		return err
	}

	log.Printf("Received records: %v", records) // Debugging: Check what records are received
	for _, record := range records {
		if len(record) >= 18 && record[0] == "DATA" {
			log.Println("Valid data record found, processing...")
			marketData, err := parseMarketData(record)
			if err != nil {
				log.Printf("Error parsing market data: %v", err)
				continue
			}
			log.Printf("Parsed Market Data: %+v", marketData)
			go processMarketData(marketData)
		} else {
			log.Printf("Ignoring non-data or malformed record: %v", record)
		}
	}

	// Assuming you have some processing here
	return nil
}

func parseMarketData(record []string) (*MarketData, error) {
	if len(record) < 18 {
		return nil, fmt.Errorf("record too short, expected at least 18 fields, got %d", len(record))
	}
	// Parse fields as required, converting types and handling errors.
	marketData := MarketData{
		// Assume all parsing succeeds, add error handling as necessary
		Symbol:      record[1],
		LTP:         parseFloat(record[2]),
		LTQ:         parseInt(record[3]),
		Volume:      parseInt(record[4]),
		LastTrdTime: parseInt64(record[5]),
		BidQ:        parseInt(record[6]),
		Bid:         parseFloat(record[7]),
		Ask:         parseFloat(record[8]),
		AskQ:        parseInt(record[9]),
		Open:        parseFloat(record[10]),
		High:        parseFloat(record[11]),
		Low:         parseFloat(record[12]),
		PClose:      parseFloat(record[13]),
		OI:          parseInt(record[14]),
		AvgPrice:    parseFloat(record[15]),
		Exch:        record[16],
		Token:       parseInt(record[17]),
	}
	return &marketData, nil
}

func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Printf("Error parsing float from '%s': %v", s, err)
		return 0.0
	}
	return f
}

func parseInt(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Printf("Error parsing int from '%s': %v", s, err)
		return 0
	}
	return i
}

func parseInt64(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Printf("Error parsing int64 from '%s': %v", s, err)
		return 0
	}
	return i
}

func mainF2(s string) {
	floatValue, err := strconv.ParseFloat(s, 64)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	formattedValue := fmt.Sprintf("%.2f", floatValue)

	fmt.Println(formattedValue)
}

func Round(x float64, precision int) float64 {
	shift := math.Pow(10, float64(precision))
	return math.Round(x*shift) / shift
}

func processMarketData(data *MarketData) {
	// Your processing logic here, such as updating data stores or triggering other actions
	log.Printf("Processing completed for data: %+v", data)
	ch := data.LTP - data.PClose
	chp := (ch / data.PClose) * 100

	value, _ := swapDgMap.Load(data.Symbol)
	dgData, _ := value.(dgPlusConfig)
	wsResponse := WSResponse{
		Bid:    data.Bid,
		Ask:    data.Ask,
		CH:     Round(ch, 2),
		CHP:    Round(chp, 2),
		LTP:    data.LTP,
		LTT:    strconv.FormatInt(data.LastTrdTime, 10), // Assuming you need a string representation
		UC:     nil,                                     // Specify if you have this information
		LC:     nil,                                     // Specify if you have this information
		Open:   data.Open,
		High:   data.High,
		Low:    data.Low,
		Close:  data.PClose,
		Volume: float64(data.Volume),
		OI:     float64(data.OI),
		TBQ:    float64(data.BidQ), // Assuming total bid quantity
		TSQ:    float64(data.AskQ), // Assuming total sell quantity
		Symbol: dgData.TradingSymbol,
		Depth:  nil, // Specify if you have this information
	}
	//fmt.Println("wsResponse ========", wsResponse)
	if SwapDataFeedWSWorker.IsRunning && !isExpired(dgData.Expiry) {
		dgPlusWSResponse.Store(data.Symbol, wsResponse)
		//AddDataToTickDataStorage(data.Symbol, wsResponse)
		AddDataToTickDataStorage(dgData.TradingSymbol, wsResponse)
	}
}

func StopSwapDataFeedWorker() {
	SwapDataFeedWSWorker.Mutex.Lock()
	if !SwapDataFeedWSWorker.IsRunning {
		SwapDataFeedWSWorker.Mutex.Unlock()
		return // Already stopped
	}

	SwapDataFeedWSWorker.IsRunning = false
	SwapDataFeedWSWorker.Mutex.Unlock() // Unlock before potentially blocking operations

	// Cancel the context to stop all goroutines
	cancel()

	// Wait for a short period to ensure all connections are closed
	time.Sleep(2 * time.Second)

	log.Println("Worker has been signaled to stop.")
	return
}
