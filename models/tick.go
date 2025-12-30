// models/tick.go
package models

import (
	"context"
	"fmt"
	"time"
	"trading-worker-api/db"
)

type Tick struct {
	Symbol     string
	Ask        float64
	Bid        float64
	Ltp        float64
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     float64
	Timestamp  time.Time
	ExpiryDate string
	Exchange   string
}

func (t *Tick) Save() error {
	if db.DB == nil {
		return fmt.Errorf("database pool is nil")
	}

	// For CRYPTO and FOREX exchanges, only insert if expiry is 'NO EXPIRY'
	if (t.Exchange == "CRYPTO" || t.Exchange == "FOREX") && t.ExpiryDate != "NO EXPIRY" {
		return nil // Skip insertion
	}

	_, err := db.DB.Exec(context.Background(),
		`INSERT INTO history (symbol, ask, bid, ltp, open, high, low, close, volume, timestamp, expiry_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		t.Symbol, t.Ask, t.Bid, t.Ltp, t.Open, t.High, t.Low, t.Close, t.Volume, t.Timestamp, t.ExpiryDate,
	)
	return err
}
