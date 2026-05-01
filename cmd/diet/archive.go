package main

import (
	"archive/tar"
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Manifest is the diet-export/manifest.json header — a single source of truth
// for what's in the archive. Diet and Directus versions are advisory: import
// does not enforce a match, but the diff command surfaces them for visibility.
type Manifest struct {
	DietVersion      string         `json:"diet_version"`
	DirectusVersion  string         `json:"directus_version"`
	SourceURL        string         `json:"source_url"`
	ExportedAt       string         `json:"exported_at"`
	Format           string         `json:"format"`
	Collections      []string       `json:"collections"`
	ItemCounts       map[string]int `json:"item_counts"`
	SystemEntities   []string       `json:"system_entities,omitempty"`
}

// SchemaBundle holds everything needed to recreate the schema half of an
// archive. Item data lives outside this bundle, one file per collection.
type SchemaBundle struct {
	Collections []CollectionInfo `json:"collections"`
	Fields      []FieldInfo      `json:"fields"`
	Relations   []RelationInfo   `json:"relations"`
}

// Create archive

func createArchive(format, outputPath string, manifest Manifest, schema SchemaBundle, data, systemData map[string][]json.RawMessage) error {
	switch format {
	case "zstd":
		return createTarZstd(outputPath, manifest, schema, data, systemData)
	case "zip":
		return createZip(outputPath, manifest, schema, data, systemData)
	default:
		return fmt.Errorf("unknown format: %s", format)
	}
}

func createTarZstd(outputPath string, manifest Manifest, schema SchemaBundle, data, systemData map[string][]json.RawMessage) (retErr error) {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func() {
		if e := f.Close(); retErr == nil {
			retErr = e
		}
	}()

	// SpeedBestCompression: archives are written once and read rarely, so
	// trade encode time for smaller files (often 30-40% over default).
	zw, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return err
	}
	defer func() {
		if e := zw.Close(); retErr == nil {
			retErr = e
		}
	}()

	tw := tar.NewWriter(zw)
	defer func() {
		if e := tw.Close(); retErr == nil {
			retErr = e
		}
	}()

	now := time.Now()
	addFile := func(name string, content []byte) error {
		return tw.WriteHeader(&tar.Header{
			Name:    "diet-export/" + name,
			Size:    int64(len(content)),
			Mode:    0o644,
			ModTime: now,
		})
	}
	writeFile := func(name string, v any) error {
		content, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		if err := addFile(name, content); err != nil {
			return err
		}
		_, err = tw.Write(content)
		return err
	}

	if err := writeFile("manifest.json", manifest); err != nil {
		return err
	}
	if err := writeFile("schema/collections.json", schema.Collections); err != nil {
		return err
	}
	if err := writeFile("schema/fields.json", schema.Fields); err != nil {
		return err
	}
	if err := writeFile("schema/relations.json", schema.Relations); err != nil {
		return err
	}

	for col, items := range data {
		if err := writeFile("data/"+col+".json", items); err != nil {
			return err
		}
	}

	for entity, items := range systemData {
		if err := writeFile("system/"+entity+".json", items); err != nil {
			return err
		}
	}

	return nil
}

func createZip(outputPath string, manifest Manifest, schema SchemaBundle, data, systemData map[string][]json.RawMessage) (retErr error) {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func() {
		if e := f.Close(); retErr == nil {
			retErr = e
		}
	}()

	zw := zip.NewWriter(f)
	defer func() {
		if e := zw.Close(); retErr == nil {
			retErr = e
		}
	}()

	writeFile := func(name string, v any) error {
		content, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		w, err := zw.Create("diet-export/" + name)
		if err != nil {
			return err
		}
		_, err = w.Write(content)
		return err
	}

	if err := writeFile("manifest.json", manifest); err != nil {
		return err
	}
	if err := writeFile("schema/collections.json", schema.Collections); err != nil {
		return err
	}
	if err := writeFile("schema/fields.json", schema.Fields); err != nil {
		return err
	}
	if err := writeFile("schema/relations.json", schema.Relations); err != nil {
		return err
	}

	for col, items := range data {
		if err := writeFile("data/"+col+".json", items); err != nil {
			return err
		}
	}

	for entity, items := range systemData {
		if err := writeFile("system/"+entity+".json", items); err != nil {
			return err
		}
	}

	return nil
}

// Extract archive

func extractArchive(inputPath string) (Manifest, SchemaBundle, map[string][]json.RawMessage, map[string][]json.RawMessage, error) {
	if strings.HasSuffix(inputPath, ".zip") {
		return extractZip(inputPath)
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return Manifest{}, SchemaBundle{}, nil, nil, err
	}
	defer f.Close()
	return extractTarZstd(f)
}

func extractTarZstd(r io.Reader) (Manifest, SchemaBundle, map[string][]json.RawMessage, map[string][]json.RawMessage, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return Manifest{}, SchemaBundle{}, nil, nil, err
	}
	defer zr.Close()

	tr := tar.NewReader(zr)
	files := make(map[string][]byte)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Manifest{}, SchemaBundle{}, nil, nil, err
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return Manifest{}, SchemaBundle{}, nil, nil, err
		}
		// Strip "diet-export/" prefix.
		name := strings.TrimPrefix(hdr.Name, "diet-export/")
		files[name] = content
	}

	return parseArchiveFiles(files)
}

func extractZip(zipPath string) (Manifest, SchemaBundle, map[string][]json.RawMessage, map[string][]json.RawMessage, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return Manifest{}, SchemaBundle{}, nil, nil, err
	}
	defer zr.Close()

	files := make(map[string][]byte)
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return Manifest{}, SchemaBundle{}, nil, nil, err
		}
		content, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return Manifest{}, SchemaBundle{}, nil, nil, err
		}
		name := strings.TrimPrefix(f.Name, "diet-export/")
		files[name] = content
	}

	return parseArchiveFiles(files)
}

func parseArchiveFiles(files map[string][]byte) (Manifest, SchemaBundle, map[string][]json.RawMessage, map[string][]json.RawMessage, error) {
	var manifest Manifest
	if err := json.Unmarshal(files["manifest.json"], &manifest); err != nil {
		return manifest, SchemaBundle{}, nil, nil, fmt.Errorf("parse manifest: %w", err)
	}

	var schema SchemaBundle
	if d, ok := files["schema/collections.json"]; ok {
		if err := json.Unmarshal(d, &schema.Collections); err != nil {
			return manifest, schema, nil, nil, fmt.Errorf("parse collections: %w", err)
		}
	}
	if d, ok := files["schema/fields.json"]; ok {
		if err := json.Unmarshal(d, &schema.Fields); err != nil {
			return manifest, schema, nil, nil, fmt.Errorf("parse fields: %w", err)
		}
	}
	if d, ok := files["schema/relations.json"]; ok {
		if err := json.Unmarshal(d, &schema.Relations); err != nil {
			return manifest, schema, nil, nil, fmt.Errorf("parse relations: %w", err)
		}
	}

	data := make(map[string][]json.RawMessage)
	for name, content := range files {
		if strings.HasPrefix(name, "data/") && strings.HasSuffix(name, ".json") {
			col := strings.TrimSuffix(path.Base(name), ".json")
			var items []json.RawMessage
			if err := json.Unmarshal(content, &items); err != nil {
				return manifest, schema, nil, nil, fmt.Errorf("parse data/%s.json: %w", col, err)
			}
			data[col] = items
		}
	}

	systemData := make(map[string][]json.RawMessage)
	for name, content := range files {
		if strings.HasPrefix(name, "system/") && strings.HasSuffix(name, ".json") {
			entity := strings.TrimSuffix(path.Base(name), ".json")
			var items []json.RawMessage
			if err := json.Unmarshal(content, &items); err != nil {
				return manifest, schema, nil, nil, fmt.Errorf("parse system/%s.json: %w", entity, err)
			}
			systemData[entity] = items
		}
	}

	return manifest, schema, data, systemData, nil
}

// archiveSize returns a human-readable file size.
func archiveSize(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return "?"
	}
	size := info.Size()
	switch {
	case size < 1024:
		return fmt.Sprintf("%d B", size)
	case size < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(size)/1024)
	default:
		return fmt.Sprintf("%.1f MB", float64(size)/(1024*1024))
	}
}

