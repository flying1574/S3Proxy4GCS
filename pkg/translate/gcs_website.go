package translate

import (
	"log/slog"

	"cloud.google.com/go/storage"
)

// TranslateS3ToGCSWebsite maps S3 WebsiteConfiguration to GCS BucketWebsite
func TranslateS3ToGCSWebsite(s3Cfg WebsiteConfiguration) *storage.BucketWebsite {
	slog.Info("Translating S3 Website Configuration")

	gcsWebsite := &storage.BucketWebsite{}

	if s3Cfg.IndexDocument != nil {
		gcsWebsite.MainPageSuffix = s3Cfg.IndexDocument.Suffix
		slog.Debug("Website MainPageSuffix", "suffix", gcsWebsite.MainPageSuffix)
	}

	if s3Cfg.ErrorDocument != nil {
		gcsWebsite.NotFoundPage = s3Cfg.ErrorDocument.Key
		slog.Debug("Website NotFoundPage", "page", gcsWebsite.NotFoundPage)
	}

	return gcsWebsite
}

// TranslateGCSToS3Website converts GCS BucketWebsite to S3 WebsiteConfiguration XML struct.
func TranslateGCSToS3Website(gcsWebsite *storage.BucketWebsite) *WebsiteConfiguration {
	if gcsWebsite == nil || (gcsWebsite.MainPageSuffix == "" && gcsWebsite.NotFoundPage == "") {
		return nil
	}

	s3Cfg := &WebsiteConfiguration{}

	if gcsWebsite.MainPageSuffix != "" {
		s3Cfg.IndexDocument = &IndexDocument{Suffix: gcsWebsite.MainPageSuffix}
	}

	if gcsWebsite.NotFoundPage != "" {
		s3Cfg.ErrorDocument = &ErrorDocument{Key: gcsWebsite.NotFoundPage}
	}

	return s3Cfg
}
