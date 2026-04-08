package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Settings contains all the configuration for the proxy
type Settings struct {
	Port                string
	GCPProjectID        string
	TargetBucket        string
	StorageBaseURL      string // For testing or custom setups
	GCSPrefix           string // Subfolder prefix for testing or namespacing
	DryRun              bool   // DryRun mode disables real GCS API hits
	DebugLogging        bool   // DebugLogging enables verbose output
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConcurrentRequests int    // Throttle middleware limit; 0 = no limit
	ProxyAccessKey        string // For re-signing
	ProxySecretKey      string // For re-signing
	JSONKey             string // Path to GCS Service Account JSON key
}

var Config *Settings

// LoadConfig initialize the settings from a .env file or environment variables
func LoadConfig() {
	// Load from .env if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, reading from environment variables directly.")
	}

	dryRunStr := getEnv("DRY_RUN", "true") // Default to true if not set (Safe for laptop testing)
	dryRun := dryRunStr == "true"
	debugLogging := getEnv("DEBUG_LOGGING", "false") == "true"

	maxIdleConns := 1000
	if v := getEnv("MAX_IDLE_CONNS", "1000"); v != "" {
		if n, err := strconv.Atoi(v); err != nil {
			log.Printf("WARNING: invalid MAX_IDLE_CONNS value %q, using default 1000", v)
		} else {
			maxIdleConns = n
		}
	}

	maxIdleConnsPerHost := 1000
	if v := getEnv("MAX_IDLE_CONNS_PER_HOST", "1000"); v != "" {
		if n, err := strconv.Atoi(v); err != nil {
			log.Printf("WARNING: invalid MAX_IDLE_CONNS_PER_HOST value %q, using default 1000", v)
		} else {
			maxIdleConnsPerHost = n
		}
	}

	maxConcurrentRequests := 1000
	if v := getEnv("MAX_CONCURRENT_REQUESTS", "1000"); v != "" {
		if n, err := strconv.Atoi(v); err != nil {
			log.Printf("WARNING: invalid MAX_CONCURRENT_REQUESTS value %q, using default 1000", v)
		} else if n > 0 {
			maxConcurrentRequests = n
		} else {
			maxConcurrentRequests = 0 // 0 means disabled
		}
	}

	Config = &Settings{
		Port:                  getEnv("PORT", "8080"),
		GCPProjectID:          getEnv("GCP_PROJECT_ID", ""),
		TargetBucket:          getEnv("TARGET_BUCKET", ""),
		StorageBaseURL:        getEnv("STORAGE_BASE_URL", "https://storage.googleapis.com"),
		GCSPrefix:             getEnv("GCS_PREFIX", ""),
		DryRun:                dryRun,
		DebugLogging:          debugLogging,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		MaxConcurrentRequests: maxConcurrentRequests,
		ProxyAccessKey:      getEnv("PROXY_AWS_ACCESS_KEY_ID", getEnv("AWS_ACCESS_KEY_ID", "")),
		ProxySecretKey:      getEnv("PROXY_AWS_SECRET_ACCESS_KEY", getEnv("AWS_SECRET_ACCESS_KEY", "")),
		JSONKey:             getEnv("JSON_KEY", ""),
	}

	// Validate required fields for non-DryRun mode
	if !dryRun {
		if Config.TargetBucket == "" {
			log.Fatal("FATAL: TARGET_BUCKET is required when DRY_RUN=false")
		}
		if Config.GCPProjectID == "" {
			log.Println("WARNING: GCP_PROJECT_ID is empty, some GCS operations may fail")
		}
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
