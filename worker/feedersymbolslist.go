package worker

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

type FeederSymbolListResponse struct {
	Error   bool     `json:"error"`
	Message string   `json:"message"`
	Symbols []string `json:"symbols"`
}

func FetchFeederSymbolList() ([]string, error) {
	// Define the WebSocket URL // feeder
	u := url.URL{Scheme: "ws", Host: Env.FEEDER_WS_URL, Path: "/ws"}
	log.Printf("Connecting to %s", u.String())

	// Set up a dialer with a timeout
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second // Set a handshake timeout

	// Create header and add authentication token
	headers := http.Header{}
	headers.Add("x-api-key", Env.FEEDER_WS_TOKEN)

	// Connect to the WebSocket server
	//c, _, err := dialer.Dial(u.String(), nil)
	c, _, err := dialer.Dial(u.String(), headers)
	if err != nil {
		log.Printf("Failed to connect to WebSocket: %v", err)
		return nil, err
	}
	log.Println("Connected to feeder WebSocket")
	defer c.Close()

	// Prepare the payload
	payload := map[string]string{"event": "symbol_list"}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to marshal payload: %v", err)
		return nil, err
	}

	// Send the payload to the WebSocket server
	err = c.WriteMessage(websocket.TextMessage, payloadBytes)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return nil, err
	}

	// Set a read deadline to avoid hanging indefinitely
	err = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		log.Printf("Failed to set read deadline: %v", err)
		return nil, err
	}

	// Read the response from the WebSocket server
	_, message, err := c.ReadMessage()
	if err != nil {
		log.Printf("Failed to read message: %v", err)
		return nil, err
	}

	// Unmarshal the response
	var response FeederSymbolListResponse
	err = json.Unmarshal(message, &response)
	if err != nil {
		log.Printf("Failed to unmarshal response: %v", err)
		return nil, err
	}

	// Check for errors in the response
	if response.Error {
		log.Printf("WebSocket server returned an error: %s", response.Message)
		return nil, errors.New(response.Message)
	}

	// Return the list of symbols
	return response.Symbols, nil
}
