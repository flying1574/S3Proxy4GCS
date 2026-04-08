package translate

import (
	"log/slog"
	"strings"
)

const S3TagPrefix = "s3tag-"

// TranslateS3ToGCSTagging computes the metadata update map for GCS.
// It clears existing s3tag- keys by setting them to "" and sets new ones.
func TranslateS3ToGCSTagging(s3Cfg Tagging, existingMetadata map[string]string) map[string]string {
	slog.Info("Translating S3 Object Tagging to GCS Metadata")

	updateMap := make(map[string]string)

	// 1. Mark all existing s3tag- keys for deletion (set to "")
	for k := range existingMetadata {
		if strings.HasPrefix(k, S3TagPrefix) {
			updateMap[k] = ""
		}
	}

	// 2. Set new tags
	for _, tag := range s3Cfg.TagSet {
		// Replace characters if necessary, but assume standard keys for now.
		// AWS allows alphanumeric, spaces, and + - = . _ : /
		// GCS allows standard HTTP header characters.
		k := S3TagPrefix + tag.Key
		updateMap[k] = tag.Value
		slog.Debug("Tag translated", "key", k, "value", tag.Value)
	}

	return updateMap
}

// TranslateGCSToS3Tagging converts GCS object metadata back to S3 Tagging XML
func TranslateGCSToS3Tagging(metadata map[string]string) *Tagging {
	t := &Tagging{}
	for k, v := range metadata {
		if strings.HasPrefix(strings.ToLower(k), strings.ToLower(S3TagPrefix)) {
			key := k[len(S3TagPrefix):]
			if key == "" {
				slog.Warn("Skipping GCS metadata entry with empty tag key after prefix", "raw_key", k)
				continue
			}
			t.TagSet = append(t.TagSet, Tag{Key: key, Value: v})
		}
	}
	return t
}
