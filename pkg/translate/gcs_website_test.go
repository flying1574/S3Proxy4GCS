package translate

import (
	"encoding/xml"
	"testing"

	"cloud.google.com/go/storage"
)

func TestTranslateS3ToGCSWebsite(t *testing.T) {
	xmlInput := `<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <IndexDocument>
        <Suffix>index.html</Suffix>
    </IndexDocument>
    <ErrorDocument>
        <Key>error.html</Key>
    </ErrorDocument>
</WebsiteConfiguration>`

	var s3Cfg WebsiteConfiguration
	err := xml.Unmarshal([]byte(xmlInput), &s3Cfg)
	if err != nil {
		t.Fatalf("Failed to unmarshal XML: %v", err)
	}

	gcsWebsite := TranslateS3ToGCSWebsite(s3Cfg)

	if gcsWebsite == nil {
		t.Fatalf("Expected non-nil GCS Website settings")
	}

	if gcsWebsite.MainPageSuffix != "index.html" {
		t.Errorf("Expected MainPageSuffix 'index.html', got '%s'", gcsWebsite.MainPageSuffix)
	}

	if gcsWebsite.NotFoundPage != "error.html" {
		t.Errorf("Expected NotFoundPage 'error.html', got '%s'", gcsWebsite.NotFoundPage)
	}
}

func TestTranslateS3ToGCSWebsitePartial(t *testing.T) {
	xmlInput := `<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <IndexDocument>
        <Suffix>index.html</Suffix>
    </IndexDocument>
</WebsiteConfiguration>`

	var s3Cfg WebsiteConfiguration
	err := xml.Unmarshal([]byte(xmlInput), &s3Cfg)
	if err != nil {
		t.Fatalf("Failed to unmarshal XML: %v", err)
	}

	gcsWebsite := TranslateS3ToGCSWebsite(s3Cfg)

	if gcsWebsite == nil {
		t.Fatalf("Expected non-nil GCS Website settings")
	}

	if gcsWebsite.MainPageSuffix != "index.html" {
		t.Errorf("Expected MainPageSuffix 'index.html', got '%s'", gcsWebsite.MainPageSuffix)
	}

	if gcsWebsite.NotFoundPage != "" {
		t.Errorf("Expected empty NotFoundPage, got '%s'", gcsWebsite.NotFoundPage)
	}
}

func TestTranslateGCSToS3Website_Full(t *testing.T) {
	gcsWebsite := &storage.BucketWebsite{
		MainPageSuffix: "index.html",
		NotFoundPage:   "error.html",
	}

	s3Cfg := TranslateGCSToS3Website(gcsWebsite)
	if s3Cfg == nil {
		t.Fatal("Expected non-nil WebsiteConfiguration")
	}
	if s3Cfg.IndexDocument == nil || s3Cfg.IndexDocument.Suffix != "index.html" {
		t.Errorf("Expected IndexDocument.Suffix 'index.html', got %v", s3Cfg.IndexDocument)
	}
	if s3Cfg.ErrorDocument == nil || s3Cfg.ErrorDocument.Key != "error.html" {
		t.Errorf("Expected ErrorDocument.Key 'error.html', got %v", s3Cfg.ErrorDocument)
	}

	// Verify XML output
	xmlBytes, err := xml.MarshalIndent(s3Cfg, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal to XML: %v", err)
	}
	t.Logf("Generated S3 XML:\n%s", string(xmlBytes))
}

func TestTranslateGCSToS3Website_IndexOnly(t *testing.T) {
	gcsWebsite := &storage.BucketWebsite{
		MainPageSuffix: "home.html",
	}

	s3Cfg := TranslateGCSToS3Website(gcsWebsite)
	if s3Cfg == nil {
		t.Fatal("Expected non-nil WebsiteConfiguration")
	}
	if s3Cfg.IndexDocument == nil || s3Cfg.IndexDocument.Suffix != "home.html" {
		t.Error("Expected IndexDocument with Suffix 'home.html'")
	}
	if s3Cfg.ErrorDocument != nil {
		t.Errorf("Expected nil ErrorDocument, got %v", s3Cfg.ErrorDocument)
	}
}

func TestTranslateGCSToS3Website_Nil(t *testing.T) {
	s3Cfg := TranslateGCSToS3Website(nil)
	if s3Cfg != nil {
		t.Errorf("Expected nil for nil input, got %v", s3Cfg)
	}
}

func TestTranslateGCSToS3Website_Empty(t *testing.T) {
	gcsWebsite := &storage.BucketWebsite{}
	s3Cfg := TranslateGCSToS3Website(gcsWebsite)
	if s3Cfg != nil {
		t.Errorf("Expected nil for empty website, got %v", s3Cfg)
	}
}
