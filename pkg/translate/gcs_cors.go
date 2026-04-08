package translate

import (
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// TranslateS3ToGCSCors converts S3 CORS Configuration XML to GCS storage.CORS slice.
// It returns the translated GCS CORS rules and a list of AllowedHeaders that were
// dropped because GCS CORS does not support request header filtering.
func TranslateS3ToGCSCors(s3Cfg *CORSConfiguration) ([]storage.CORS, []string) {
	var gcsCors []storage.CORS
	var droppedHeaders []string

	for _, rule := range s3Cfg.CORSRules {
		var maxAge time.Duration
		if rule.MaxAgeSeconds != nil {
			maxAge = time.Duration(*rule.MaxAgeSeconds) * time.Second
		}

		if len(rule.AllowedHeaders) > 0 {
			slog.Warn("S3 AllowedHeaders (Request Headers) are not natively supported by GCS CORS translation and will be ignored.",
				"dropped_headers", rule.AllowedHeaders)
			droppedHeaders = append(droppedHeaders, rule.AllowedHeaders...)
		}

		gcsRule := storage.CORS{
			MaxAge:          maxAge,
			Methods:         rule.AllowedMethods,
			Origins:         rule.AllowedOrigins,
			ResponseHeaders: rule.ExposeHeaders,
		}

		gcsCors = append(gcsCors, gcsRule)
	}

	// Deduplicate dropped headers
	if len(droppedHeaders) > 0 {
		seen := make(map[string]bool, len(droppedHeaders))
		unique := droppedHeaders[:0]
		for _, h := range droppedHeaders {
			lower := strings.ToLower(h)
			if !seen[lower] {
				seen[lower] = true
				unique = append(unique, h)
			}
		}
		droppedHeaders = unique
	}

	return gcsCors, droppedHeaders
}

// TranslateGCSToS3Cors converts GCS CORS configuration to S3 CORSConfiguration XML
func TranslateGCSToS3Cors(gcsCors []storage.CORS) *CORSConfiguration {
	if len(gcsCors) == 0 {
		return nil
	}

	s3Cfg := &CORSConfiguration{}
	for _, rule := range gcsCors {
		var maxAge *int
		if rule.MaxAge > 0 {
			seconds := int(rule.MaxAge.Seconds())
			maxAge = &seconds
		}

		s3Rule := CORSRule{
			AllowedMethods: rule.Methods,
			AllowedOrigins: rule.Origins,
			ExposeHeaders:  rule.ResponseHeaders,
			MaxAgeSeconds:  maxAge,
		}
		s3Cfg.CORSRules = append(s3Cfg.CORSRules, s3Rule)
	}

	return s3Cfg
}
