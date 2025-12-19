package worker

import (
	"math"
	"sync"
)

type WSWorker struct {
	IsRunning bool
	Mutex     sync.Mutex
}

var ZerodhaWSWorker *WSWorker
var DGPlusWSWorker *WSWorker
var SwapDataFeedWSWorker *WSWorker
var FeederDataFeedWSWorker *WSWorker
var DiwjDataFDWWSWorker *WSWorker

type WSResponse struct {
	Bid    float64     `json:"bid"`
	Ask    float64     `json:"ask"`
	CH     float64     `json:"ch"`
	CHP    float64     `json:"chp"`
	LTP    float64     `json:"ltp"`
	LTT    string      `json:"ltt"`
	UC     interface{} `json:"uc"`
	LC     interface{} `json:"lc"`
	Open   float64     `json:"open"`
	High   float64     `json:"high"`
	Low    float64     `json:"low"`
	Close  float64     `json:"close"`
	Volume float64     `json:"volume"`
	OI     float64     `json:"oi"`
	TBQ    float64     `json:"tbq"`
	TSQ    float64     `json:"tsq"`
	Symbol string      `json:"symbol"`
	Depth  interface{} `json:"depth"`
}

func roundFloat(x float64) float64 {
	return math.Round(x*100) / 100
}
