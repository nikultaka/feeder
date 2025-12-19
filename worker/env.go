package worker

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

var Env *ENV

type ENV struct {
	APIAddress           string `env:"API_ADDRESS"`
	API_LIVE_ADDRESS     string `env:"API_LIVE_ADDRESS"`
	API_LIVE_IP          string `env:"API_LIVE_IP"`
	API_ADDRESS_URL      string `env:"API_ADDRESS_URL"`
	APIAccessUsername    string `env:"API_ACCESS_USERNAME"`
	APIAccessPassword    string `env:"API_ACCESS_PASSWORD"`
	RedisAddress         string `env:"REDIS_ADDRESS"`
	RedisPassword        string `env:"REDIS_PASSWORD"`
	RedisDB              int    `env:"REDIS_DB"`
	GoogleSpreadsheetID  string `env:"GOOGLE_SPREADSHEET_ID"`
	ClientDetailsSheetID string `env:"CLIENT_DETAILS_SHEET_ID"`
	ClientAPISheetID     string `env:"CLIENT_API_SHEET_ID"`
	ZerodhaAPIKey        string `env:"ZERODHA_API_KEY"`
	ZerodhaAPISecret     string `env:"ZERODHA_API_SECRET"`
	TickDataSleepInMS    int    `env:"TICK_DATA_SLEEP_IN_MS"`
	MaxRetries           int    `env:"MAX_RETRIES"`
	Debug                bool   `env:"DEBUG"`
	FEEDER_WS_URL        string `env:"FEEDER_WS_URL"`
	FEEDER_WS_URL_N      string `env:"FEEDER_WS_URL_N"`
	FEEDER_WS_TOKEN      string `env:"FEEDER_WS_TOKEN"`
	DIWJ_FEEDER_WS_TOKEN string `env:"DIWJ_FEEDER_WS_TOKEN"`
	DIWJ_FEEDER_WS_URL_N string `env:"DIWJ_FEEDER_WS_URL_N"`
}

func LoadENV() {
	if err := godotenv.Load(); err != nil {
		Logger.Fatal().Err(err).Msg("Error loading .env file")
	}

	tickDataSleep, err := strconv.Atoi(os.Getenv("TICK_DATA_SLEEP_IN_MS"))
	if err != nil {
		Logger.Fatal().Err(err).Msg("Failed to convert string to int for TICK_DATA_SLEEP_IN_MS")
	}

	redisDB, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		Logger.Fatal().Err(err).Msg("Failed to convert string to int for RedisTickerDB")
	}

	var maxRetries int
	maxRetries, err = strconv.Atoi(os.Getenv("MAX_RETRIES"))
	if err != nil {
		// set default value
		maxRetries = 5
		Logger.Error().Err(err).Msg("Error parsing MAX_RETRIES")
	}

	Env = &ENV{
		API_ADDRESS_URL:      os.Getenv("API_ADDRESS_URL"),
		APIAddress:           os.Getenv("API_ADDRESS"),
		APIAccessUsername:    os.Getenv("API_ACCESS_USERNAME"),
		APIAccessPassword:    os.Getenv("API_ACCESS_PASSWORD"),
		RedisAddress:         os.Getenv("REDIS_ADDRESS"),
		RedisPassword:        os.Getenv("REDIS_PASSWORD"),
		RedisDB:              redisDB,
		GoogleSpreadsheetID:  os.Getenv("GOOGLE_SPREADSHEET_ID"),
		ClientDetailsSheetID: os.Getenv("CLIENT_DETAILS_SHEET_ID"),
		ClientAPISheetID:     os.Getenv("CLIENT_API_SHEET_ID"),
		ZerodhaAPIKey:        os.Getenv("ZERODHA_API_KEY"),
		ZerodhaAPISecret:     os.Getenv("ZERODHA_API_SECRET"),
		FEEDER_WS_URL:        os.Getenv("FEEDER_WS_URL"),
		FEEDER_WS_URL_N:      os.Getenv("FEEDER_WS_URL_N"),
		FEEDER_WS_TOKEN:      os.Getenv("FEEDER_WS_TOKEN"),
		DIWJ_FEEDER_WS_TOKEN: os.Getenv("DIWJ_FEEDER_WS_TOKEN"),
		DIWJ_FEEDER_WS_URL_N: os.Getenv("DIWJ_FEEDER_WS_URL_N"),
		TickDataSleepInMS:    tickDataSleep,
		MaxRetries:           maxRetries,
		Debug:                os.Getenv("DEBUG") == "true",
	}
}
