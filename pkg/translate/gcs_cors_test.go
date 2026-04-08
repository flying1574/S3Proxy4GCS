package translate

import (
	"encoding/xml"
	"testing"
	"time"

	"cloud.google.com/go/storage"
)

func TestTranslateS3ToGCSCors(t *testing.T) {
	xmlInput := `
	<CORSConfiguration>
		<CORSRule>
			<AllowedOrigin>*</AllowedOrigin>
			<AllowedMethod>GET</AllowedMethod>
			<AllowedMethod>HEAD</AllowedMethod>
			<AllowedHeader>Authorization</AllowedHeader>
			<ExposeHeader>x-amz-request-id</ExposeHeader>
			<MaxAgeSeconds>3000</MaxAgeSeconds>
		</CORSRule>
	</CORSConfiguration>
	`

	var s3Cfg CORSConfiguration
	if err := xml.Unmarshal([]byte(xmlInput), &s3Cfg); err != nil {
		t.Fatalf("Failed to unmarshal XML input: %v", err)
	}

	gcsCors, droppedHeaders := TranslateS3ToGCSCors(&s3Cfg)

	// Verify dropped AllowedHeaders are reported
	if len(droppedHeaders) != 1 || droppedHeaders[0] != "Authorization" {
		t.Errorf("Expected droppedHeaders ['Authorization'], got %v", droppedHeaders)
	}

	if len(gcsCors) == 0 {
		t.Fatalf("Expected at least one CORS rule, got none")
	}

	var rule storage.CORS
	rule = gcsCors[0]

	if len(rule.Origins) != 1 || rule.Origins[0] != "*" {
		t.Errorf("Expected Origin '*', got %v", rule.Origins)
	}

	if len(rule.Methods) != 2 || rule.Methods[0] != "GET" || rule.Methods[1] != "HEAD" {
		t.Errorf("Expected Methods ['GET', 'HEAD'], got %v", rule.Methods)
	}

	if len(rule.ResponseHeaders) != 1 || rule.ResponseHeaders[0] != "x-amz-request-id" {
		t.Errorf("Expected ResponseHeader 'x-amz-request-id', got %v", rule.ResponseHeaders)
	}

	if rule.MaxAge != 3000*time.Second {
		t.Errorf("Expected MaxAge 3000s, got %v", rule.MaxAge)
	}
}
