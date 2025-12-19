package worker

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var Logger *zerolog.Logger

func InitLogger() {
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load IST time zone")
	}

	initLogger := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	// initLogger.FormatLevel = func(i interface{}) string {
	// 	return strings.ToUpper(fmt.Sprintf("%-6s", i))
	// }

	initLogger.FormatTimestamp = func(i interface{}) string {
		return time.Now().In(ist).Format("2006-01-02 15:04:05")
	}

	logger := zerolog.New(initLogger).With().Logger()
	Logger = &logger
}
