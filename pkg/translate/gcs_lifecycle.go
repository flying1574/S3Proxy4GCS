package translate

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/storage"
)

// GCSLifecycle represents the top-level GCS Lifecycle JSON
type GCSLifecycle struct {
	Rules []GCSLifecycleRule `json:"rule"`
}

// GCSLifecycleRule represents a single rule
type GCSLifecycleRule struct {
	Action    GCSLifecycleAction    `json:"action"`
	Condition GCSLifecycleCondition `json:"condition"`
}

// GCSLifecycleAction represents the action to take (e.g., Delete, SetStorageClass)
type GCSLifecycleAction struct {
	Type         string `json:"type"`                   // Delete, SetStorageClass
	StorageClass string `json:"storageClass,omitempty"` // e.g. NEARLINE, COLDLINE, ARCHIVE
}

// GCSLifecycleCondition represents the conditions when the action should trigger
type GCSLifecycleCondition struct {
	Age                 *int     `json:"age,omitempty"`
	CreatedBefore       *string  `json:"createdBefore,omitempty"` // yyyy-mm-dd
	IsLive              *bool    `json:"isLive,omitempty"`
	MatchesStorageClass []string `json:"matchesStorageClass,omitempty"`
	MatchesPrefix       []string `json:"matchesPrefix,omitempty"`
	MatchesSuffix       []string `json:"matchesSuffix,omitempty"`
	NumNewerVersions    *int     `json:"numNewerVersions,omitempty"`
}

// TranslateS3ToGCS Lifecycle takes an S3 LifecycleConfiguration and returns a GCS JSON byte slice and an error.
func TranslateS3ToGCS(s3Cfg *LifecycleConfiguration) ([]byte, error) {
	var gcsCfg GCSLifecycle

	for _, s3Rule := range s3Cfg.Rules {
		if s3Rule.Status != "Enabled" {
			continue
		}

		// Translate Expirations
		if s3Rule.Expiration != nil {
			rule := GCSLifecycleRule{
				Action: GCSLifecycleAction{Type: "Delete"},
			}
			if err := applyRuleFilter(s3Rule.Filter, &rule.Condition); err != nil {
				return nil, err
			}

			if s3Rule.Expiration.Days != nil {
				rule.Condition.Age = s3Rule.Expiration.Days
			}
			if s3Rule.Expiration.Date != nil {
				gcsDate := formatDateS3toGCS(*s3Rule.Expiration.Date)
				rule.Condition.CreatedBefore = &gcsDate
			}
			gcsCfg.Rules = append(gcsCfg.Rules, rule)
		}

		// Translate Transitions
		for _, trans := range s3Rule.Transitions {
			rule := GCSLifecycleRule{
				Action: GCSLifecycleAction{
					Type:         "SetStorageClass",
					StorageClass: mapStorageClass(trans.StorageClass),
				},
			}
			if err := applyRuleFilter(s3Rule.Filter, &rule.Condition); err != nil {
				return nil, err
			}

			if trans.Days != nil {
				rule.Condition.Age = trans.Days
			}
			if trans.Date != nil {
				gcsDate := formatDateS3toGCS(*trans.Date)
				rule.Condition.CreatedBefore = &gcsDate
			}
			gcsCfg.Rules = append(gcsCfg.Rules, rule)
		}

		// Translate Noncurrent Version Expirations
		if s3Rule.NoncurrentVersionExpirations != nil {
			rule := GCSLifecycleRule{
				Action: GCSLifecycleAction{Type: "Delete"},
				Condition: GCSLifecycleCondition{
					NumNewerVersions: s3Rule.NoncurrentVersionExpirations.NoncurrentDays,
					IsLive:           boolPtr(false),
				},
			}
			if err := applyRuleFilter(s3Rule.Filter, &rule.Condition); err != nil {
				return nil, err
			}
			gcsCfg.Rules = append(gcsCfg.Rules, rule)
		}
	}

	return json.MarshalIndent(gcsCfg, "", "  ")
}

func applyRuleFilter(f *Filter, c *GCSLifecycleCondition) error {
	if f == nil {
		return nil
	}

	if f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil {
		return fmt.Errorf("ObjectSize filters (ObjectSizeGreaterThan/ObjectSizeLessThan) are not supported by GCS Lifecycle translation")
	}

	if f.Tag != nil {
		return fmt.Errorf("Tag filters are not supported by GCS Lifecycle translation")
	}

	if f.Prefix != nil && *f.Prefix != "" {
		c.MatchesPrefix = []string{*f.Prefix}
	}

	if f.And != nil {
		if f.And.ObjectSizeGreaterThan != nil || f.And.ObjectSizeLessThan != nil {
			return fmt.Errorf("ObjectSize filters in And are not supported by GCS Lifecycle translation")
		}
		if len(f.And.Tags) > 0 {
			return fmt.Errorf("Tag filters in And are not supported by GCS Lifecycle translation")
		}
		if f.And.Prefix != nil && *f.And.Prefix != "" {
			c.MatchesPrefix = append(c.MatchesPrefix, *f.And.Prefix)
		}
	}

	return nil
}

func mapStorageClass(s3Class string) string {
	switch s3Class {
	case "STANDARD_IA":
		return "NEARLINE"
	case "ONEZONE_IA":
		return "NEARLINE"
	case "INTELLIGENT_TIERING":
		return "STANDARD"
	case "GLACIER", "GLACIER_IR":
		return "COLDLINE"
	case "DEEP_ARCHIVE":
		return "ARCHIVE"
	default:
		return "STANDARD"
	}
}

func formatDateS3toGCS(s3Date string) string {
	if len(s3Date) >= 10 {
		return s3Date[:10] // Take yyyy-mm-dd
	}
	return s3Date
}

func boolPtr(b bool) *bool {
	return &b
}

// TranslateS3ToGCSLifecycle converts S3 LifecycleConfiguration directly to a GCS SDK storage.Lifecycle struct,
// bypassing the intermediate JSON representation to avoid field-name mismatches.
func TranslateS3ToGCSLifecycle(s3Cfg *LifecycleConfiguration) (*storage.Lifecycle, error) {
	var lifecycle storage.Lifecycle

	for _, s3Rule := range s3Cfg.Rules {
		if s3Rule.Status != "Enabled" {
			continue
		}

		// Validate filter
		if s3Rule.Filter != nil {
			if s3Rule.Filter.ObjectSizeGreaterThan != nil || s3Rule.Filter.ObjectSizeLessThan != nil {
				return nil, fmt.Errorf("ObjectSize filters are not supported by GCS Lifecycle translation")
			}
			if s3Rule.Filter.Tag != nil {
				return nil, fmt.Errorf("Tag filters are not supported by GCS Lifecycle translation")
			}
			if s3Rule.Filter.And != nil {
				if s3Rule.Filter.And.ObjectSizeGreaterThan != nil || s3Rule.Filter.And.ObjectSizeLessThan != nil {
					return nil, fmt.Errorf("ObjectSize filters in And are not supported by GCS Lifecycle translation")
				}
				if len(s3Rule.Filter.And.Tags) > 0 {
					return nil, fmt.Errorf("Tag filters in And are not supported by GCS Lifecycle translation")
				}
			}
		}

		// Translate Expiration
		if s3Rule.Expiration != nil {
			rule := storage.LifecycleRule{
				Action: storage.LifecycleAction{Type: storage.DeleteAction},
			}
			applySDKFilter(s3Rule.Filter, &rule.Condition)
			if s3Rule.Expiration.Days != nil {
				rule.Condition.AgeInDays = int64(*s3Rule.Expiration.Days)
			}
			lifecycle.Rules = append(lifecycle.Rules, rule)
		}

		// Translate Transitions
		for _, trans := range s3Rule.Transitions {
			rule := storage.LifecycleRule{
				Action: storage.LifecycleAction{
					Type:         storage.SetStorageClassAction,
					StorageClass: mapStorageClass(trans.StorageClass),
				},
			}
			applySDKFilter(s3Rule.Filter, &rule.Condition)
			if trans.Days != nil {
				rule.Condition.AgeInDays = int64(*trans.Days)
			}
			lifecycle.Rules = append(lifecycle.Rules, rule)
		}

		// Translate NoncurrentVersionExpirations
		if s3Rule.NoncurrentVersionExpirations != nil {
			rule := storage.LifecycleRule{
				Action: storage.LifecycleAction{Type: storage.DeleteAction},
				Condition: storage.LifecycleCondition{
					Liveness: storage.Archived,
				},
			}
			if s3Rule.NoncurrentVersionExpirations.NoncurrentDays != nil {
				rule.Condition.NumNewerVersions = int64(*s3Rule.NoncurrentVersionExpirations.NoncurrentDays)
			}
			applySDKFilter(s3Rule.Filter, &rule.Condition)
			lifecycle.Rules = append(lifecycle.Rules, rule)
		}

		// Translate AbortIncompleteMultipartUpload
		if s3Rule.AbortIncompleteMultipartUpload != nil && s3Rule.AbortIncompleteMultipartUpload.DaysAfterInitiation != nil {
			rule := storage.LifecycleRule{
				Action: storage.LifecycleAction{Type: storage.AbortIncompleteMPUAction},
				Condition: storage.LifecycleCondition{
					AgeInDays: int64(*s3Rule.AbortIncompleteMultipartUpload.DaysAfterInitiation),
				},
			}
			lifecycle.Rules = append(lifecycle.Rules, rule)
		}
	}

	return &lifecycle, nil
}

// applySDKFilter sets MatchesPrefix on a storage.LifecycleCondition from an S3 Filter.
func applySDKFilter(f *Filter, c *storage.LifecycleCondition) {
	if f == nil {
		return
	}
	if f.Prefix != nil && *f.Prefix != "" {
		c.MatchesPrefix = []string{*f.Prefix}
	}
	if f.And != nil && f.And.Prefix != nil && *f.And.Prefix != "" {
		c.MatchesPrefix = append(c.MatchesPrefix, *f.And.Prefix)
	}
}

// TranslateGCSToS3Lifecycle converts GCS storage.Lifecycle to S3 LifecycleConfiguration XML struct.
func TranslateGCSToS3Lifecycle(gcsLifecycle storage.Lifecycle) *LifecycleConfiguration {
	s3Cfg := &LifecycleConfiguration{}

	for i, gcsRule := range gcsLifecycle.Rules {
		s3Rule := Rule{
			ID:     fmt.Sprintf("rule-%d", i),
			Status: "Enabled",
		}

		// Translate condition prefix to Filter
		if len(gcsRule.Condition.MatchesPrefix) > 0 {
			prefix := gcsRule.Condition.MatchesPrefix[0]
			s3Rule.Filter = &Filter{Prefix: &prefix}
		}

		switch gcsRule.Action.Type {
		case storage.DeleteAction:
			if gcsRule.Condition.Liveness == storage.Archived {
				// NoncurrentVersionExpiration
				nve := &NoncurrentVersionExpiration{}
				if gcsRule.Condition.NumNewerVersions > 0 {
					days := int(gcsRule.Condition.NumNewerVersions)
					nve.NoncurrentDays = &days
				}
				s3Rule.NoncurrentVersionExpirations = nve
			} else {
				// Standard Expiration
				exp := &Expiration{}
				if gcsRule.Condition.AgeInDays > 0 {
					days := int(gcsRule.Condition.AgeInDays)
					exp.Days = &days
				}
				if !gcsRule.Condition.CreatedBefore.IsZero() {
					date := gcsRule.Condition.CreatedBefore.Format("2006-01-02T15:04:05.000Z")
					exp.Date = &date
				}
				s3Rule.Expiration = exp
			}
		case storage.SetStorageClassAction:
			trans := Transition{
				StorageClass: reverseMapStorageClass(gcsRule.Action.StorageClass),
			}
			if gcsRule.Condition.AgeInDays > 0 {
				days := int(gcsRule.Condition.AgeInDays)
				trans.Days = &days
			}
			if !gcsRule.Condition.CreatedBefore.IsZero() {
				date := gcsRule.Condition.CreatedBefore.Format("2006-01-02T15:04:05.000Z")
				trans.Date = &date
			}
			s3Rule.Transitions = append(s3Rule.Transitions, trans)
		case storage.AbortIncompleteMPUAction:
			if gcsRule.Condition.AgeInDays > 0 {
				days := int(gcsRule.Condition.AgeInDays)
				s3Rule.AbortIncompleteMultipartUpload = &AbortIncompleteMultipartUpload{
					DaysAfterInitiation: &days,
				}
			}
		}

		s3Cfg.Rules = append(s3Cfg.Rules, s3Rule)
	}

	return s3Cfg
}

// reverseMapStorageClass maps GCS storage class back to S3 equivalent.
func reverseMapStorageClass(gcsClass string) string {
	switch gcsClass {
	case "NEARLINE":
		return "STANDARD_IA"
	case "COLDLINE":
		return "GLACIER"
	case "ARCHIVE":
		return "DEEP_ARCHIVE"
	case "STANDARD":
		return "STANDARD"
	default:
		return gcsClass
	}
}
