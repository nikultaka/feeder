package worker

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var dgPlusScriptIDs []string
var dgPlusWSResponse sync.Map

type DGPlusGetAllXMLResponse struct {
	XMLName xml.Name `xml:"string"`
	Data    string   `xml:",innerxml"`
}

type DGPlusScriptData struct {
	ScripID string `json:"scripid"`
	A1      string `json:"a1"`
	A2      string `json:"a2"`
	A3      string `json:"a3"`
	A4      string `json:"a4"`
	A5      string `json:"a5"`
	A6      string `json:"a6"`
	A7      string `json:"a7"`
	A8      string `json:"a8"`
	A9      string `json:"a9"`
	A10     string `json:"a10"`
	A11     string `json:"a11"`
	A12     string `json:"a12"`
	A13     string `json:"a13"`
	A14     string `json:"a14"`
	A15     string `json:"a15"`
	A16     string `json:"a16"`
	A17     string `json:"a17"`
	A18     string `json:"a18"`
	A19     string `json:"a19"`
	A20     string `json:"a20"`
	A21     string `json:"a21"`
	A22     string `json:"a22"`
	A23     string `json:"a23"`
	A24     string `json:"a24"`
	A25     string `json:"a25"`
	A26     string `json:"a26"`
	A27     string `json:"a27"`
	A28     string `json:"a28"`
	A29     string `json:"a29"`
	A30     string `json:"a30"`
	A31     string `json:"a31"`
	A32     string `json:"a32"`
	A33     string `json:"a33"`
	A34     string `json:"a34"`
	A35     string `json:"a35"`
	A36     string `json:"a36"`
	A37     string `json:"a37"`
	A38     string `json:"a38"`
	A39     string `json:"a39"`
	A40     string `json:"a40"`
	A41     string `json:"a41"`
	A42     string `json:"a42"`
	A43     string `json:"a43"`
	A44     string `json:"a44"`
	A45     string `json:"a45"`
	A46     string `json:"a46"`
	A47     string `json:"a47"`
}

func GetDGPlusData(scriptIDs []string) ([]DGPlusScriptData, error) {
	var dgPlusScriptData []DGPlusScriptData

	strScriptIDs := strings.Join(scriptIDs, ",")
	loginURL := "http://api.dgpluslive.com/Vxnd2swX/GetFeed.asmx/GetALL?scrips=" + strScriptIDs

	req, err := http.NewRequest("GET", loginURL, nil)
	if err != nil {
		Logger.Error().Err(err).Msg("[getDGPlusData] Error creating request")
		return dgPlusScriptData, err
	}

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		Logger.Error().Err(err).Msg("[getDGPlusData] Error making request")
		return dgPlusScriptData, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		Logger.Error().Err(err).Msg("[getDGPlusData] Error reading response body")
		return dgPlusScriptData, err
	}

	var dgPlusResponse DGPlusGetAllXMLResponse
	err = xml.Unmarshal(body, &dgPlusResponse)
	if err != nil {
		Logger.Error().Err(err).Msg("[getDGPlusData] Error decoding XML response")
		return dgPlusScriptData, err
	}

	err = json.Unmarshal([]byte(dgPlusResponse.Data), &dgPlusScriptData)
	if err != nil {
		Logger.Error().Err(err).Msg("[getDGPlusData] Error decoding JSON response")
		return dgPlusScriptData, err
	}

	return dgPlusScriptData, nil
}

func extractDGPlusFloatValue(prevValue float64, newValue string, isChange bool) (float64, bool) {
	floatValue, err := strconv.ParseFloat(newValue, 64)
	if err != nil {
		return prevValue, isChange
	}
	roundFloatValue := roundFloat(floatValue)
	if isChange {
		return roundFloatValue, isChange
	}

	if prevValue != roundFloatValue {
		return roundFloatValue, true
	}
	return roundFloatValue, isChange
}

func dgPlusWSHandler(retry int) {

	for {
		time.Sleep(500 * time.Millisecond)
		if !DGPlusWSWorker.IsRunning {
			time.Sleep(1 * time.Second)
			return
		}

		message, err := GetDGPlusData(dgPlusScriptIDs)
		if err != nil {
			Logger.Error().Err(err).Msg("[DG] [WS] Failed to get GetDGPlusData")
			if retry < 20 {
				Logger.Error().Any("retry", retry+1).Msg(fmt.Sprintf("[DG] [WS] Reconnecting to DG Plus websocket after %d seconds", 2*retry))
				time.Sleep(time.Duration(2*retry) * time.Second)
				go dgPlusWSHandler(retry + 1)
				time.Sleep(2 * time.Second)
			} else {
				DGPlusWSWorker.Mutex.Lock()
				defer DGPlusWSWorker.Mutex.Unlock()
				DGPlusWSWorker.IsRunning = false
			}
			return
		}

		var isChange bool
		for _, scriptData := range message {
			value, ok := dgPlusWSResponse.Load(scriptData.ScripID)
			if !ok {
				continue
			}
			finalData, ok := value.(WSResponse)
			if !ok {
				continue
			}
			isChange = false
			finalData.Bid, isChange = extractDGPlusFloatValue(finalData.Bid, scriptData.A6, isChange)
			finalData.Ask, isChange = extractDGPlusFloatValue(finalData.Ask, scriptData.A7, isChange)

			if finalData.Bid == 0 || finalData.Ask == 0 {
				continue
			}

			finalData.CH, isChange = extractDGPlusFloatValue(finalData.CH, scriptData.A11, isChange)
			finalData.CHP, isChange = extractDGPlusFloatValue(finalData.CHP, scriptData.A19, isChange)
			finalData.LTP, isChange = extractDGPlusFloatValue(finalData.LTP, scriptData.A9, isChange)
			finalData.Open, isChange = extractDGPlusFloatValue(finalData.Open, scriptData.A12, isChange)
			finalData.High, isChange = extractDGPlusFloatValue(finalData.High, scriptData.A13, isChange)
			finalData.Low, isChange = extractDGPlusFloatValue(finalData.Low, scriptData.A14, isChange)
			finalData.Close, isChange = extractDGPlusFloatValue(finalData.Close, scriptData.A15, isChange)
			finalData.Volume, isChange = extractDGPlusFloatValue(finalData.Volume, scriptData.A4, isChange)
			finalData.OI, isChange = extractDGPlusFloatValue(finalData.OI, scriptData.A21, isChange)
			finalData.TBQ, isChange = extractDGPlusFloatValue(finalData.TBQ, scriptData.A26, isChange)
			finalData.TSQ, isChange = extractDGPlusFloatValue(finalData.TSQ, scriptData.A27, isChange)

			if isChange {
				finalData.LTT = GetUTCISODatetime()
				AddDataToTickDataStorage(finalData.Symbol, finalData)
				dgPlusWSResponse.Store(scriptData.ScripID, finalData)
			}
		}
	}
}

func StartDGPlusWorker() {
	DGPlusWSWorker.Mutex.Lock()
	defer DGPlusWSWorker.Mutex.Unlock()
	DGPlusWSWorker.IsRunning = true

	PublishSymbolSheetData()

	var tickData WSResponse
	dgPlusScriptIDs = []string{}
	for _, row := range DGPlusConfigData {
		tickData = WSResponse{
			Bid:    0,
			Ask:    0,
			CH:     0,
			CHP:    0,
			LTP:    0,
			Open:   0,
			High:   0,
			Low:    0,
			Close:  0,
			Volume: 0,
			OI:     0,
			TBQ:    0,
			TSQ:    0,
			Symbol: row.TradingSymbol,
			Depth:  nil,
		}
		dgPlusWSResponse.Store(row.DGPlusID, tickData)
		dgPlusScriptIDs = append(dgPlusScriptIDs, row.DGPlusID)
	}

	Logger.Printf("[StartDGPlusWorker] Total symbols: %d\n", len(ClientSheetDetailsConfigData))
	go dgPlusWSHandler(0)
}

func StopDGPlusWorker() {
	DGPlusWSWorker.Mutex.Lock()
	defer DGPlusWSWorker.Mutex.Unlock()
	DGPlusWSWorker.IsRunning = false
	time.Sleep(1 * time.Second)
}
