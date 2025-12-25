package context

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type Configuration struct {
	MetaQuery string `json:"metaquery"`
	Host      string `json:"host"`
	Port      int16  `json:"port"`
	User      string `json:"user"`
	Database  string `json:"database"`
	Password  string `json:"password"`
}

// Global Configuration
var GlobalConfigCtx *Configuration = &Configuration{}

// On initialization, reads the configs passed and store them in "config.json"
func (confCtx *Configuration) SaveToFile() {
	_, err := os.OpenFile("config.json", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Panic("error occurred while creating session file")
	}

	data, _ := json.Marshal(confCtx)
	os.WriteFile("config.json", data, 0644)
}

// Reads "config.json" and initializes the global configuration
func LoadSession() error {
	data, err := os.ReadFile("config.json")
	if err != nil {
		return err
	}

	json.Unmarshal(data, &GlobalConfigCtx)
	fmt.Println("Loaded Session Details")

	return nil
}
