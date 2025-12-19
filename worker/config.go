package worker

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

var ClientSheetDetailsConfigData []clientSheetDetailsConfig
var ClientAPIDetailsConfigData []clientAPIDetailsConfig
var ZerodhaConfigData map[uint32]zerodhaConfig
var DGPlusConfigData map[string]dgPlusConfig
var FeederConfigData map[string]feederConfig
var DiwjDataFDWConfigData map[string]diwjDataFDWConfig

type zerodhaConfig struct {
	InstrumentToken    uint32 `json:"instrument_token"`
	ExchangeToken      string `json:"exchange_token"`
	TradingSymbol      string `json:"trading_symbol"`
	Exchange           string `json:"exchange"`
	LowerCircuitLevel1 int    `json:"lower_circuit_level_1"`
	LowerCircuitLevel2 int    `json:"lower_circuit_level_2"`
	LowerCircuitLevel3 int    `json:"lower_circuit_level_3"`
	UpperCircuitLevel1 int    `json:"upper_circuit_level_1"`
	UpperCircuitLevel2 int    `json:"upper_circuit_level_2"`
	UpperCircuitLevel3 int    `json:"upper_circuit_level_3"`
}

type diwjDataFDWConfig struct {
	TradingSymbol       string `json:"trading_symbol"`
	TradingName         string `json:"name"`
	SubscribeSymbolName string `json:"subscribe_symbol_name"`
	Exchange            string `json:"exchange"`
	Expiry              string `json:"expiry"`
}

type feederConfig struct {
	InstrumentToken     uint32 `json:"instrument_token"`
	ExchangeToken       string `json:"exchange_token"`
	TradingSymbol       string `json:"trading_symbol"`
	TradingName         string `json:"name"`
	SubscribeSymbolName string `json:"subscribe_symbol_name"`
	Exchange            string `json:"exchange"`
	Expiry              string `json:"expiry"`
}

type dgPlusConfig struct {
	DGPlusID      string `json:"dg_plus_id"`
	TradingSymbol string `json:"trading_symbol"`
	TradingName   string `json:"name"`
	Expiry        string `json:"expiry"`
}

type clientSheetDetailsConfig struct {
	ClientUsername string `json:"client_username"`
	SheetName      string `json:"sheet_name"`
	Type           string `json:"type"`
	SpreadsheetID  string `json:"spreadsheet_id"`
	SheetID        string `json:"sheet_id"`
}

type clientAPIDetailsConfig struct {
	ClientUsername    string `json:"client_username"`
	ServerAddress     string `json:"server_address"`
	APIAccessToken    string `json:"api_access_token"`
	WSConnectionLimit int    `json:"ws_connection_limit"`
}

func getGoogleSheetCSVData(spreadsheetID string, sheetID string) ([][]string, error) {
	var rows [][]string
	csvSheetUrl := fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/export?gid=%s&format=csv", spreadsheetID, sheetID)
	//fmt.Println("CSV sheet URL:", csvSheetUrl)

	// Make an HTTP GET request to the URL
	resp, err := http.Get(csvSheetUrl)
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to fetch CSV file from getGoogleSheetCSVData")
		return rows, err
	}
	defer resp.Body.Close()

	// Read the response body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to read response body")
		return rows, err
	}

	//fmt.Println("CSV data:", string(data))
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(string(data)))
	rows, err = reader.ReadAll()
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to parse CSV data")
		return rows, err
	}

	// removing header row
	rows = rows[1:]
	return rows, err
}

func LoadClientSheetDetailsConfig(spreadsheetID string, sheetID string) error {
	ClientSheetDetailsConfigData = []clientSheetDetailsConfig{}
	ZerodhaConfigData = map[uint32]zerodhaConfig{}
	DGPlusConfigData = map[string]dgPlusConfig{}
	FeederConfigData = map[string]feederConfig{}
	DiwjDataFDWConfigData = map[string]diwjDataFDWConfig{}

	rows, err := getGoogleSheetCSVData(spreadsheetID, sheetID)
	if err != nil {
		log.Println("err", rows)
		return err
	}
	var clientSpreadsheetID string
	var clientSheetID string
	var clientSheetType string
	for _, row := range rows {
		clientSheetType = strings.TrimSpace(row[2])
		clientSpreadsheetID = strings.TrimSpace(row[3])
		clientSheetID = strings.TrimSpace(row[4])
		ClientSheetDetailsConfigData = append(ClientSheetDetailsConfigData, clientSheetDetailsConfig{
			ClientUsername: strings.TrimSpace(row[0]),
			SheetName:      strings.TrimSpace(row[1]),
			Type:           clientSheetType,
			SpreadsheetID:  clientSpreadsheetID,
			SheetID:        clientSheetID,
		})

		clientRows, err := getGoogleSheetCSVData(clientSpreadsheetID, clientSheetID)
		if err != nil {
			return err
		}
		for _, clientRow := range clientRows {
			if clientSheetType == "zerodha" {
				if strings.TrimSpace(clientRow[0]) != "" {
					token, err := strconv.Atoi(strings.TrimSpace(clientRow[0]))
					if err != nil {
						Logger.Error().Err(err).Msg("[loadClientSheetDetailsConfig] [zerodha] failed to convert string to int instrument_token")
						continue
					}
					circuitLvl1 := strings.TrimSpace(clientRow[21])
					circuitLvl2 := strings.TrimSpace(clientRow[22])
					circuitLvl3 := strings.TrimSpace(clientRow[23])

					lowerCircuitLevel1 := 0
					lowerCircuitLevel2 := 0
					lowerCircuitLevel3 := 0
					upperCircuitLevel1 := 0
					upperCircuitLevel2 := 0
					upperCircuitLevel3 := 0

					if circuitLvl1 != "" {
						circuitLvlSplit := strings.Split(circuitLvl1, "-")
						lowerCircuitLevel1, err = strconv.Atoi(circuitLvlSplit[0])
						if err != nil {
							Logger.Error().Err(err).Msg("[zerodha] failed to convert string to int lowerCircuitLevel1")
						}
						upperCircuitLevel1, err = strconv.Atoi(circuitLvlSplit[1])
						if err != nil {
							Logger.Error().Err(err).Msg("[zerodha] failed to convert string to int upperCircuitLevel1")
						}

						if circuitLvl2 != "" {
							circuitLvlSplit = strings.Split(circuitLvl2, "-")
							lowerCircuitLevel2, err = strconv.Atoi(circuitLvlSplit[0])
							if err != nil {
								Logger.Error().Err(err).Msg("[zerodha] failed to convert string to int lowerCircuitLevel2")
							}
							upperCircuitLevel2, err = strconv.Atoi(circuitLvlSplit[1])
							if err != nil {
								Logger.Error().Err(err).Msg("[zerodha] failed to convert string to int upperCircuitLevel2")
							}
							if circuitLvl3 != "" {
								circuitLvlSplit = strings.Split(circuitLvl3, "-")
								lowerCircuitLevel3, err = strconv.Atoi(circuitLvlSplit[0])
								if err != nil {
									Logger.Error().Err(err).Msg("[zerodha] failed to convert string to int lowerCircuitLevel3")
								}
								upperCircuitLevel3, err = strconv.Atoi(circuitLvlSplit[1])
								if err != nil {
									Logger.Error().Err(err).Msg("[zerodha] failed to convert string to int upperCircuitLevel3")
								}
							}
						}
					}

					zData := zerodhaConfig{
						InstrumentToken:    uint32(token),
						ExchangeToken:      strings.TrimSpace(clientRow[1]),
						TradingSymbol:      strings.TrimSpace(clientRow[2]),
						Exchange:           strings.TrimSpace(clientRow[11]),
						LowerCircuitLevel1: lowerCircuitLevel1,
						LowerCircuitLevel2: lowerCircuitLevel2,
						LowerCircuitLevel3: lowerCircuitLevel3,
						UpperCircuitLevel1: upperCircuitLevel1,
						UpperCircuitLevel2: upperCircuitLevel2,
						UpperCircuitLevel3: upperCircuitLevel3,
					}
					ZerodhaConfigData[uint32(token)] = zData
				}
			}

			if clientSheetType == "dgplus" {
				if strings.TrimSpace(clientRow[0]) != "" {
					DGPlusConfigData[strings.TrimSpace(clientRow[0])] = dgPlusConfig{
						DGPlusID:      strings.TrimSpace(clientRow[0]),
						TradingSymbol: strings.TrimSpace(clientRow[1]),
						TradingName:   strings.TrimSpace(clientRow[2]),
						Expiry:        strings.TrimSpace(clientRow[3]),
					}
				}
			}

			if clientSheetType == "feeder" {
				if strings.TrimSpace(clientRow[0]) != "" {
					token, err := strconv.Atoi(strings.TrimSpace(clientRow[0]))
					if err != nil {
						Logger.Error().Err(err).Msg("[loadClientSheetDetailsConfig] [zerodha] failed to convert string to int instrument_token")
						continue
					}
					log.Println("clientRow[4]  .", clientRow[4])
					FeederConfigData[strings.TrimSpace(clientRow[0])] = feederConfig{
						InstrumentToken:     uint32(token),
						ExchangeToken:       strings.TrimSpace(clientRow[1]),
						TradingSymbol:       strings.TrimSpace(clientRow[2]),
						TradingName:         strings.TrimSpace(clientRow[3]),
						SubscribeSymbolName: strings.TrimSpace(clientRow[4]),
						Expiry:              strings.TrimSpace(clientRow[6]),
						Exchange:            strings.TrimSpace(clientRow[12]),
					}
				}
			}
			if clientSheetType == "diwjdatafdw" {
				if strings.TrimSpace(clientRow[0]) != "" {
					// token, err := strconv.Atoi(strings.TrimSpace(clientRow[0]))
					// if err != nil {
					// 	Logger.Error().Err(err).Msg("[loadClientSheetDetailsConfig] [zerodha] failed to convert string to int instrument_token")
					// 	continue
					// }
					log.Println("clientRow[4]  .", clientRow[4])
					DiwjDataFDWConfigData[strings.TrimSpace(clientRow[0])] = diwjDataFDWConfig{
						TradingSymbol:       strings.TrimSpace(clientRow[2]),
						TradingName:         strings.TrimSpace(clientRow[3]),
						SubscribeSymbolName: strings.TrimSpace(clientRow[4]),
						Expiry:              strings.TrimSpace(clientRow[6]),
						Exchange:            strings.TrimSpace(clientRow[12]),
					}
				}
			}
		}
	}
	return nil
}

func LoadClientAPIDetailsConfig(spreadsheetID string, sheetID string) error {
	ClientAPIDetailsConfigData = []clientAPIDetailsConfig{}

	rows, err := getGoogleSheetCSVData(spreadsheetID, sheetID)
	if err != nil {
		return err
	}
	for _, row := range rows {
		wsConnectionLimit, err := strconv.Atoi(row[3])
		if err != nil {
			return err
		}

		ClientAPIDetailsConfigData = append(ClientAPIDetailsConfigData, clientAPIDetailsConfig{
			ClientUsername:    strings.TrimSpace(row[0]),
			ServerAddress:     strings.TrimSpace(row[1]),
			APIAccessToken:    strings.TrimSpace(row[2]),
			WSConnectionLimit: wsConnectionLimit,
		})
	}
	return nil

}
