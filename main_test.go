package main

import (
	"testing"
	"trading-worker-api/worker"

	"github.com/gin-gonic/gin"
)

func TestZerodhaConfig(t *testing.T) {
	worker.LoadENV()

	if !worker.Env.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	err := worker.LoadClientSheetDetailsConfig("1aAVTsrFT8J4iNtbwW2izXLUNzcp15hiaRqiGsHBIhRE", "205702328")
	if err != nil {
		t.Errorf("Error: %s\n", err)
	}
	worker.Logger.Printf("\n%+v\n", worker.ZerodhaConfigData)
}

func TestCurrentTimeFormat(t *testing.T) {
	worker.Logger.Print(worker.GetISTISODatetime())
	worker.Logger.Print(worker.GetUTCISODatetime())
}
