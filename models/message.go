package models

import (
	"encoding/json"
	"fmt"
	"time"
)

type Message struct {
	ID      int       `json:"id"`
	Content string    `json:"content"`
	SentAt  time.Time `json:"sent_at"`
}

// Serialize MyMessage -> []byte
func (m Message) Serialize() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		fmt.Printf("[Message] Failed to serialize message: %v\n", err)
		return nil, err
	}
	return data, nil
}

// Deserialize []byte -> MyMessage
func Deserialize(data []byte) (Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		fmt.Printf("[Message] Failed to deserialize message: %v\n", err)
		return Message{}, err
	}
	return msg, nil
}
