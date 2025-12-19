package worker

import (
	"net/http"
	"time"
)

func IsAPILive(url string, accessToken string) bool {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}

	return resp.StatusCode == http.StatusOK
}

func GetISTISODatetime() string {
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		Logger.Error().Err(err).Msg("Error loading IST time zone")
		return ""
	}

	// Get the current time in IST
	currentTime := time.Now().In(ist)

	// Format the time in ISO 8601 format
	isoTime := currentTime.Format("2006-01-02T15:04:05-07:00")

	return isoTime
}

func GetUTCISODatetime() string {
	// Get the current time in IST
	currentTime := time.Now().UTC()

	// Format the time in ISO 8601 format
	isoTime := currentTime.Format("2006-01-02T15:04:05-07:00")

	return isoTime
}
