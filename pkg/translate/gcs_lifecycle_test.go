package translate

import (
	"encoding/xml"
	"testing"
	"time"

	"cloud.google.com/go/storage"
)

func TestTranslateS3ToGCS(t *testing.T) {
	xmlInput := `
	<LifecycleConfiguration>
		<Rule>
			<ID>TestRule</ID>
			<Status>Enabled</Status>
			<Filter>
				<Prefix>logs/</Prefix>
			</Filter>
			<Transition>
				<Days>30</Days>
				<StorageClass>GLACIER</StorageClass>
			</Transition>
			<Expiration>
				<Days>365</Days>
			</Expiration>
		</Rule>
	</LifecycleConfiguration>
	`

	var s3Cfg LifecycleConfiguration
	if err := xml.Unmarshal([]byte(xmlInput), &s3Cfg); err != nil {
		t.Fatalf("Failed to unmarshal XML input: %v", err)
	}

	gcsJSON, err := TranslateS3ToGCS(&s3Cfg)
	if err != nil {
		t.Fatalf("Failed to translate S3 to GCS: %v", err)
	}

	t.Logf("Generated GCS JSON:\n%s", string(gcsJSON))

	// Simple assertions
	if len(gcsJSON) == 0 {
		t.Error("Generated JSON is empty")
	}

	// Double check some keywords are present
	jsonStr := string(gcsJSON)
	if !contains(jsonStr, `"Delete"`) {
		t.Error(`Expected Action type "Delete"`)
	}
	if !contains(jsonStr, `"SetStorageClass"`) {
		t.Error(`Expected Action type "SetStorageClass"`)
	}
	if !contains(jsonStr, `"COLDLINE"`) {
		t.Error(`Expected GCS StorageClass mapping "COLDLINE" for GLACIER`)
	}
}

func contains(s, substr string) bool {
	// Simple string contains for testing without heavy deps
	return len(s) >= len(substr) && func() bool {
		for i := 0; i < len(s)-len(substr)+1; i++ {
			if s[i:i+len(substr)] == substr {
				return true
			}
		}
		return false
	}()
}
func TestTranslateS3ToGCS_PrefixFilter(t *testing.T) {
	xmlInput := `
	<LifecycleConfiguration>
		<Rule>
			<ID>PrefixRule</ID>
			<Status>Enabled</Status>
			<Filter>
				<Prefix>images/</Prefix>
			</Filter>
			<Expiration>
				<Days>90</Days>
			</Expiration>
		</Rule>
	</LifecycleConfiguration>
	`

	var s3Cfg LifecycleConfiguration
	if err := xml.Unmarshal([]byte(xmlInput), &s3Cfg); err != nil {
		t.Fatalf("Failed to unmarshal XML input: %v", err)
	}

	gcsJSON, err := TranslateS3ToGCS(&s3Cfg)
	if err != nil {
		t.Fatalf("Failed to translate S3 to GCS: %v", err)
	}

	jsonStr := string(gcsJSON)
	if !contains(jsonStr, `"matchesPrefix"`) {
		t.Error(`Expected "matchesPrefix" condition`)
	}
	if !contains(jsonStr, `["images/"]`) && !contains(jsonStr, `"images/"`) { // It should be an array of strings
		t.Error(`Expected "images/" inside matchesPrefix array`)
	}
}

func TestTranslateGCSToS3Lifecycle_Delete(t *testing.T) {
	gcsLifecycle := storage.Lifecycle{
		Rules: []storage.LifecycleRule{
			{
				Action:    storage.LifecycleAction{Type: storage.DeleteAction},
				Condition: storage.LifecycleCondition{AgeInDays: 365},
			},
		},
	}

	s3Cfg := TranslateGCSToS3Lifecycle(gcsLifecycle)
	if s3Cfg == nil {
		t.Fatal("Expected non-nil S3 LifecycleConfiguration")
	}
	if len(s3Cfg.Rules) != 1 {
		t.Fatalf("Expected 1 rule, got %d", len(s3Cfg.Rules))
	}

	rule := s3Cfg.Rules[0]
	if rule.Status != "Enabled" {
		t.Errorf("Expected Status 'Enabled', got '%s'", rule.Status)
	}
	if rule.Expiration == nil {
		t.Fatal("Expected Expiration to be set")
	}
	if rule.Expiration.Days == nil || *rule.Expiration.Days != 365 {
		t.Errorf("Expected Expiration Days=365, got %v", rule.Expiration.Days)
	}

	// Verify XML marshalling works
	xmlBytes, err := xml.MarshalIndent(s3Cfg, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal to XML: %v", err)
	}
	t.Logf("Generated S3 XML:\n%s", string(xmlBytes))
}

func TestTranslateGCSToS3Lifecycle_SetStorageClass(t *testing.T) {
	gcsLifecycle := storage.Lifecycle{
		Rules: []storage.LifecycleRule{
			{
				Action: storage.LifecycleAction{
					Type:         storage.SetStorageClassAction,
					StorageClass: "NEARLINE",
				},
				Condition: storage.LifecycleCondition{
					AgeInDays:     30,
					MatchesPrefix: []string{"logs/"},
				},
			},
		},
	}

	s3Cfg := TranslateGCSToS3Lifecycle(gcsLifecycle)
	if len(s3Cfg.Rules) != 1 {
		t.Fatalf("Expected 1 rule, got %d", len(s3Cfg.Rules))
	}

	rule := s3Cfg.Rules[0]
	if len(rule.Transitions) != 1 {
		t.Fatalf("Expected 1 transition, got %d", len(rule.Transitions))
	}
	if rule.Transitions[0].StorageClass != "STANDARD_IA" {
		t.Errorf("Expected StorageClass 'STANDARD_IA', got '%s'", rule.Transitions[0].StorageClass)
	}
	if rule.Transitions[0].Days == nil || *rule.Transitions[0].Days != 30 {
		t.Errorf("Expected Days=30, got %v", rule.Transitions[0].Days)
	}
	if rule.Filter == nil || rule.Filter.Prefix == nil || *rule.Filter.Prefix != "logs/" {
		t.Error("Expected Filter with Prefix 'logs/'")
	}
}

func TestTranslateGCSToS3Lifecycle_NoncurrentVersion(t *testing.T) {
	gcsLifecycle := storage.Lifecycle{
		Rules: []storage.LifecycleRule{
			{
				Action: storage.LifecycleAction{Type: storage.DeleteAction},
				Condition: storage.LifecycleCondition{
					Liveness:         storage.Archived,
					NumNewerVersions: 3,
				},
			},
		},
	}

	s3Cfg := TranslateGCSToS3Lifecycle(gcsLifecycle)
	rule := s3Cfg.Rules[0]
	if rule.NoncurrentVersionExpirations == nil {
		t.Fatal("Expected NoncurrentVersionExpiration to be set")
	}
	if rule.NoncurrentVersionExpirations.NoncurrentDays == nil || *rule.NoncurrentVersionExpirations.NoncurrentDays != 3 {
		t.Errorf("Expected NoncurrentDays=3, got %v", rule.NoncurrentVersionExpirations.NoncurrentDays)
	}
}

func TestTranslateGCSToS3Lifecycle_CreatedBefore(t *testing.T) {
	date := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	gcsLifecycle := storage.Lifecycle{
		Rules: []storage.LifecycleRule{
			{
				Action:    storage.LifecycleAction{Type: storage.DeleteAction},
				Condition: storage.LifecycleCondition{CreatedBefore: date},
			},
		},
	}

	s3Cfg := TranslateGCSToS3Lifecycle(gcsLifecycle)
	rule := s3Cfg.Rules[0]
	if rule.Expiration == nil || rule.Expiration.Date == nil {
		t.Fatal("Expected Expiration with Date")
	}
	if !contains(*rule.Expiration.Date, "2025-06-15") {
		t.Errorf("Expected date containing '2025-06-15', got '%s'", *rule.Expiration.Date)
	}
}

func TestTranslateGCSToS3Lifecycle_Empty(t *testing.T) {
	gcsLifecycle := storage.Lifecycle{}
	s3Cfg := TranslateGCSToS3Lifecycle(gcsLifecycle)
	if len(s3Cfg.Rules) != 0 {
		t.Errorf("Expected 0 rules for empty lifecycle, got %d", len(s3Cfg.Rules))
	}
}
