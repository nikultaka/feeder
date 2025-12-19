package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"trading-worker-api/db"
	"trading-worker-api/docs"
	"trading-worker-api/worker"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/sirupsen/logrus"

	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func BasicAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		fmt.Println(c.Request.URL.Path)

		logFile, logger := createDailyLogFile()
		if logger == nil {
			return
		}
		defer logFile.Close()
		// Get request details
		method := c.Request.Method
		url := c.Request.URL.String()
		host := c.Request.Host
		remoteAddr := c.ClientIP()
		timestamp := time.Now().Format(time.RFC3339)

		// Log the request
		logger.WithFields(logrus.Fields{
			"method":    method,
			"url":       url,
			"host":      host,
			"remote":    remoteAddr,
			"timestamp": timestamp,
		}).Info("Incoming request")

		if strings.HasPrefix(c.Request.URL.Path, "/swagger") {
			// Bypass authentication for Swagger endpoints
			fmt.Println("swagger")
			c.Next()
			return
		}
		user, password, hasAuth := c.Request.BasicAuth()
		fmt.Println(user, password, hasAuth)

		if !hasAuth || user != worker.Env.APIAccessUsername || password != worker.Env.APIAccessPassword {
			c.Header("WWW-Authenticate", `Basic realm="Authorization Required"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
}

func PanicErrorHandlerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				worker.Logger.Error().Any("panic", r).Msg("Recovering from request")
			}
		}()
		c.Next()
	}
}

//	@securityDefinitions.basic	BasicAuth

// New Relic Middleware
func newRelicMiddleware(app *newrelic.Application) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start a New Relic transaction for the current request
		txn := app.StartTransaction(c.FullPath())
		defer txn.End()

		// Set the New Relic transaction in the Gin context
		c.Set("newrelic.txn", txn)

		// Continue processing the request
		c.Next()
	}
}

// Create a new log file every day
func createDailyLogFile() (*os.File, *logrus.Logger) {
	// Get the current date in the format YYYY-MM-DD
	date := time.Now().Format("2006-01-02")
	logFileName := fmt.Sprintf("logs/%s.log", date)

	// Create or open the log file for appending
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Error opening log file:", err)
		return nil, nil
	}

	// Set up logrus logger to write to the file
	logger := logrus.New()
	logger.SetOutput(file)
	// logger.SetFormatter(&logrus.TextFormatter{
	// 	FullTimestamp: true,
	// })
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	return file, logger
}

// Delete log files older than a specified period (week or month)
func cleanupOldLogFiles(retentionPeriod string) {
	// Get the current time
	now := time.Now()

	// Set retention duration (week or month)
	var duration time.Duration
	if retentionPeriod == "weekly" {
		duration = 7 * 24 * time.Hour // 7 days
	} else if retentionPeriod == "monthly" {
		duration = 30 * 24 * time.Hour // 30 days
	} else {
		fmt.Println("Invalid retention period")
		return
	}

	// Get the logs directory and check files
	logsDir := "./logs"
	files, err := os.ReadDir(logsDir)
	if err != nil {
		fmt.Println("Error reading log directory:", err)
		return
	}

	// Iterate over log files and delete the ones older than retention period
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// Get the file path
		filePath := filepath.Join(logsDir, file.Name())

		// Get the file's last modification time
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			fmt.Println("Error stating file:", err)
			continue
		}

		// If the file is older than the retention period, delete it
		if now.Sub(fileInfo.ModTime()) > duration {
			err := os.Remove(filePath)
			if err != nil {
				fmt.Println("Error deleting old log file:", err)
			} else {
				fmt.Printf("Deleted old log file: %s\n", file.Name())
			}
		}
	}
}

//	@securityDefinitions.basic	BasicAuth

// Validate Host and IP middleware
func validateHostAndIP(allowedHosts []string, allowedIPs []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Validate Host header
		requestHost := c.Request.Host
		isAllowedHost := false
		for _, host := range allowedHosts {
			if requestHost == host {
				isAllowedHost = true
				break
			}
		}
		if !isAllowedHost {
			c.JSON(403, gin.H{"error": "Forbidden: Invalid Host Header"})
			c.Abort()
			return
		}

		// Validate client IP
		clientIP := getClientIP(c.Request)

		fmt.Println("Error creating logs directory clientIP:", clientIP)
		isAllowedIP := false
		for _, ip := range allowedIPs {
			if clientIP == ip {
				isAllowedIP = true
				break
			}
		}
		if !isAllowedIP {
			c.JSON(403, gin.H{"error": "Forbidden: IP Not Allowed"})
			c.Abort()
			return
		}

		// If validation passes, continue to the next handler
		c.Next()
	}
}

// Helper function to extract client IP
func getClientIP(r *http.Request) string {
	// Check for the X-Forwarded-For header (useful if behind a proxy)
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		ips := strings.Split(xForwardedFor, ",")
		return strings.TrimSpace(ips[0]) // Return the first IP in the list
	}

	// Fallback to the remote address
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

func main() {
	// Create log directory if it doesn't exist
	if _, err := os.Stat("logs"); os.IsNotExist(err) {
		err := os.Mkdir("logs", 0755)
		if err != nil {
			fmt.Println("Error creating logs directory:", err)
			return
		}
	}
	// Create or open daily log file
	logFile, logger := createDailyLogFile()
	if logger == nil {
		return
	}
	defer logFile.Close()
	go func() {
		time.Sleep(10 * time.Second)
		cleanupOldLogFiles("monthly") // Set retention period as "weekly" or "monthly"
	}()
	worker.InitLogger()
	worker.LoadENV()
	worker.InitRedis()
	worker.InitWorkers()
	go worker.SendTickDataWorker(0)

	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("gosetup"),
		newrelic.ConfigLicense("b9edf59fedf4cfae816d860e8ff5adfcFFFFNRAL"),
		newrelic.ConfigAppLogForwardingEnabled(true),
	)
	// If an application could not be created then err will reveal why.
	if err != nil {
		fmt.Println("unable to create New Relic Application", err)
	}
	if err := db.Init(); err != nil {
		log.Fatalf("Database init failed: %v", err)
	}
	fmt.Println("app ---", app)
	r := gin.Default()

	// Apply New Relic middleware
	r.Use(newRelicMiddleware(app))
	// List of allowed hosts and IPs
	// allowedHosts := []string{os.Getenv("API_LIVE_ADDRESS"), os.Getenv("API_ADDRESS"), "192.46.212.21:8080", "122.169.102.51:8080"}
	// allowedIPs := []string{"192.46.212.21", os.Getenv("API_LIVE_IP"), "192.46.212.21", "122.169.102.51"}

	// Apply the validation middleware globally
	// r.Use(validateHostAndIP(allowedHosts, allowedIPs))
	if !worker.Env.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	worker.InitLogger()
	worker.LoadENV()
	worker.InitRedis()
	worker.InitWorkers()
	go worker.SendTickDataWorker(0)

	if !worker.Env.Debug {
		gin.SetMode(gin.ReleaseMode)
	}

	docs.SwaggerInfo.Title = "Trading Worker API"
	docs.SwaggerInfo.Description = "Trading Worker API is to manage trading worker running on server"
	docs.SwaggerInfo.Version = "1.3"
	docs.SwaggerInfo.Host = worker.Env.APIAddress
	docs.SwaggerInfo.BasePath = "/"

	// r := gin.Default()

	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"*", "http://" + worker.Env.APIAddress}
	fmt.Println("worker.Env.APIAddress ---", worker.Env.APIAddress)
	r.Use(cors.New(config))
	r.Use(BasicAuthMiddleware())

	// handle panic errors
	r.Use(PanicErrorHandlerMiddleware())

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	r.GET("/", handleHome)
	r.GET("/status", handleWorkerStatus)
	r.GET("/api-status", handleAPIStatus)
	r.GET("/update-config", handleUpdateConfig)
	r.PATCH("/start", handleStartWorker)
	r.PATCH("/stop", handleStopWorker)
	r.PATCH("/generate", handleGenerateZerodhaAccessToken)
	r.GET("/fetch-symbols", handleFetchFeederSymbolList)

	if err := r.Run(worker.Env.API_ADDRESS_URL); err != nil {
		worker.Logger.Fatal().Err(err).Msg("Error starting the server")
		fmt.Println(err)
	}

}

// handleHome godoc
// @Summary      Home
// @Security BasicAuth
// @Accept       json
// @Produce      json
// @Router       / [get]
func handleHome(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Welcome to Trading Worker API",
		"status":  true,
		"data":    nil,
	})
}

// handleWorkerStatus godoc
// @Security BasicAuth
// @Summary      worker status
// @Accept       json
// @Produce      json
// @Router       /status [get]
func handleWorkerStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "worker status",
		"status":  true,
		"data": gin.H{
			"zerodha":     map[string]bool{"isActive": worker.ZerodhaWSWorker.IsRunning},
			"dg_plus":     map[string]bool{"isActive": worker.DGPlusWSWorker.IsRunning},
			"swap":        map[string]bool{"isActive": worker.SwapDataFeedWSWorker.IsRunning},
			"feeder":      map[string]bool{"isActive": worker.FeederWSWorker.IsRunning},
			"diwjdatafdw": map[string]bool{"isActive": worker.DiwjDataFDWWSWorker.IsRunning},
		},
	})
}

// handleFetchFeederSymbolList godoc
// @Security BasicAuth
// @Summary      fetch feeder symbol list
// @Accept       json
// @Produce      json
// @Router       /fetch-symbols [get]
func handleFetchFeederSymbolList(c *gin.Context) {
	symbols, err := worker.FetchFeederSymbolList()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": err.Error(),
			"status":  false,
			"data":    nil,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Feeder symbols list",
		"status":  true,
		"data":    symbols,
	})
}

// handleAPIStatus godoc
// @Security BasicAuth
// @Summary      live API status
// @Accept       json
// @Produce      json
// @Router       /api-status [get]
func handleAPIStatus(c *gin.Context) {
	var responseData []map[string]interface{}
	if len(worker.ClientAPIDetailsConfigData) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Start any worker to get API status",
			"status":  false,
			"data":    nil,
		})
		return
	}

	for _, api := range worker.ClientAPIDetailsConfigData {
		responseData = append(responseData, map[string]interface{}{
			"client":            api.ClientUsername,
			"serverAddress":     api.ServerAddress,
			"accessToken":       api.APIAccessToken,
			"wsConnectionLimit": api.WSConnectionLimit,
			"isAlive":           worker.IsAPILive("http://"+api.ServerAddress, api.APIAccessToken),
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"message": "API status",
		"status":  true,
		"data":    responseData,
	})
}

// handleUpdateConfig godoc
// @Security BasicAuth
// @Summary      update configuration
// @Accept       json
// @Produce      json
// @Router       /update-config [get]
func handleUpdateConfig(c *gin.Context) {
	worker.PublishSymbolSheetData()
	c.JSON(http.StatusOK, gin.H{
		"message": "Configuration updated",
		"status":  true,
		"data":    nil,
	})
}

// handleStartWorker godoc
// @Security BasicAuth
// @Summary      start worker
// @Accept       json
// @Produce      json
// @Param        worker    query     string  true  "select worker"  Enums(zerodha, dg_plus, swap, feeder)
// @Router       /start [patch]
func handleStartWorker(c *gin.Context) {
	value := c.Query("worker")
	if value == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "worker query parameter required", "status": false})
		return
	}

	switch value {
	case "zerodha":
		if worker.ZerodhaWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{
				"message": "Zerodha worker is active",
				"status":  true,
				"data":    nil,
			})
			return
		}

		if worker.GetZerodhaAccessTokenFromLocalRedis() == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"message": "Zerodha access token is not generated",
				"status":  false,
				"data":    nil,
			})
			return
		}

		worker.StartZerodhaWorker()

		c.JSON(http.StatusOK, gin.H{
			"message": "Zerodha worker started",
			"status":  true,
			"data":    nil,
		})
	case "dg_plus":
		if worker.DGPlusWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{
				"message": "DG Plus worker is active",
				"status":  true,
				"data":    nil,
			})
			return
		}

		worker.StartDGPlusWorker()

		c.JSON(http.StatusOK, gin.H{
			"message": "DG Plus worker started",
			"status":  true,
			"data":    nil,
		})
	case "swap":
		if worker.SwapDataFeedWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{"message": "Swap worker is already active", "status": true, "data": nil})
			return
		}
		worker.StartSwapDataFeedWorker()
		select {
		case <-time.After(3 * time.Second):
			c.JSON(http.StatusOK, gin.H{"message": "Swap worker started", "status": true, "data": nil})
		case <-worker.SwapDataFeedWorkerSuccessChan:
			c.JSON(http.StatusOK, gin.H{"message": "Swap worker started", "status": true, "data": nil})
		}
	case "feeder":
		if worker.FeederWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{"message": "Feeder worker is already active", "status": true, "data": nil})
			return
		}
		worker.StartFeederWorker()
		c.JSON(http.StatusOK, gin.H{"message": "Feeder worker started", "status": true, "data": nil})
		select {
		case <-time.After(3 * time.Second):
			c.JSON(http.StatusOK, gin.H{"message": "Feeder worker started", "status": true, "data": nil})
		}
	case "diwjdatafdw":
		if worker.DiwjDataFDWWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{"message": "Diwjdatafdw worker is already active", "status": true, "data": nil})
			return
		}
		worker.StartDiwjDataFDWWorker()
		c.JSON(http.StatusOK, gin.H{"message": "Diwjdatafdw worker started", "status": true, "data": nil})
		select {
		case <-time.After(3 * time.Second):
			c.JSON(http.StatusOK, gin.H{"message": "Diwjdatafdw worker started", "status": true, "data": nil})
		}
	default:
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid worker name", "status": false, "data": nil})
	}
}

// handleStopWorker godoc
// @Security BasicAuth
// @Summary      stop worker
// @Accept       json
// @Produce      json
// @Param        worker    query     string  true  "select worker"  Enums(zerodha, dg_plus, swap, feeder)
// @Router       /stop [patch]
func handleStopWorker(c *gin.Context) {
	value := c.Query("worker")
	if value == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "worker query parameter required, values=[zerodha,dg_plus,swap,feeder]",
			"status":  false,
			"data":    nil,
		})
		return
	}

	if value == "zerodha" {
		if !worker.ZerodhaWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{
				"message": "Zerodha worker is not active",
				"status":  true,
				"data":    nil,
			})
			return
		}

		worker.StopZerodhaWorker()
		c.JSON(http.StatusOK, gin.H{
			"message": "Zerodha worker stopped",
			"status":  true,
			"data":    nil,
		})

	} else if value == "dg_plus" {
		if !worker.DGPlusWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{
				"message": "DG Plus worker is not active",
				"status":  true,
				"data":    nil,
			})
			return
		}

		worker.StopDGPlusWorker()
		c.JSON(http.StatusOK, gin.H{
			"message": "DG Plus worker stopped",
			"status":  true,
			"data":    nil,
		})

	} else if value == "swap" {
		if !worker.SwapDataFeedWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{
				"message": "Swap worker is not active",
				"status":  true,
				"data":    nil,
			})
			return
		}
		worker.StopSwapDataFeedWorker()
		c.JSON(http.StatusOK, gin.H{
			"message": "Swap worker stopped",
			"status":  true,
			"data":    nil,
		})
	} else if value == "feeder" {
		if !worker.FeederWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{
				"message": "Feeder worker is not active",
				"status":  true,
				"data":    nil,
			})
			return
		}
		worker.StopFeederWorker()
		c.JSON(http.StatusOK, gin.H{
			"message": "Feeder worker stopped",
			"status":  true,
			"data":    nil,
		})

	} else if value == "diwjdatafdw" {
		if !worker.DiwjDataFDWWSWorker.IsRunning {
			c.JSON(http.StatusOK, gin.H{
				"message": "Diwjdatafdw worker is not active",
				"status":  true,
				"data":    nil,
			})
			return
		}
		worker.StopDiwjDataFDWWorker()
		c.JSON(http.StatusOK, gin.H{
			"message": "Diwjdatafdw worker stopped",
			"status":  true,
			"data":    nil,
		})

	} else {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid query parameter for worker, expected values=[zerodha,dg_plus,swap,feeder]",
			"status":  false,
			"data":    nil,
		})
		return
	}
}

// handleGenerateZerodhaAccessToken godoc
// @Security BasicAuth
// @Summary      generate zerodha access token
// @Description  generates new Zerodha access token using requestToken and restart the worker
// @Accept       json
// @Produce      json
// @Param        requestToken  query  string  true  "enter zerodha request token"
// @Router       /generate [patch]
func handleGenerateZerodhaAccessToken(c *gin.Context) {
	requestToken := c.Query("requestToken")
	if requestToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "requestToken parameter required",
			"status":  false,
			"data":    nil,
		})
		return
	}

	if err := worker.UpdateZerodhaAccessToken(worker.Env.ZerodhaAPIKey, worker.Env.ZerodhaAPISecret, requestToken); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": err.Error(),
			"status":  false,
			"data":    nil,
		})
		return
	}

	if worker.ZerodhaWSWorker.IsRunning {
		worker.StopZerodhaWorker()
	}

	if worker.GetZerodhaAccessTokenFromLocalRedis() == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Failed to generate Zerodha access token",
			"status":  false,
			"data":    nil,
		})
		return
	}

	worker.StartZerodhaWorker()

	c.JSON(http.StatusOK, gin.H{
		"message": "Zerodha access token generated and restarted the Zerodha worker",
		"status":  true,
		"data":    nil,
	})
}
