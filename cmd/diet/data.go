package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// Import: insert data with multi-pass retry

type dataProgress struct {
	total    int
	inserted atomic.Int64
	failed   atomic.Int64
	pass     int
}

// applyData inserts data into target Directus with multi-pass retry.
//
// Why multi-pass: collections may have circular FK dependencies, and Directus's
// batch insert is atomic — a single FK violation rolls back the whole batch.
// We try batches of BatchSize first (fast path), then fall back to per-item
// inserts to isolate which rows are blocked. Whatever still fails is deferred
// to the next pass, by which time the row it was waiting on may exist.
// After RetryPasses, anything left is counted as permanently failed.
func applyData(client *apiClient, order []string, dataMap map[string][]json.RawMessage,
	aliasFields map[string]map[string]bool, log func(string)) *dataProgress {
	progress := &dataProgress{}
	for _, items := range dataMap {
		progress.total += len(items)
	}

	// Prepare items: strip alias fields.
	type colData struct {
		name  string
		items []json.RawMessage
	}
	var allData []colData
	for _, col := range order {
		items, ok := dataMap[col]
		if !ok || len(items) == 0 {
			continue
		}
		colAliases := aliasFields[col]
		stripped := make([]json.RawMessage, len(items))
		for i, item := range items {
			stripped[i] = stripDataFields(item, colAliases)
			stripped[i] = fixDateTimeFields(stripped[i])
		}
		allData = append(allData, colData{name: col, items: stripped})
	}

	maxPasses := client.RetryPasses
	sem := make(chan struct{}, client.Concurrency)

	for pass := 1; pass <= maxPasses; pass++ {
		progress.pass = pass
		passInserted := 0
		passRemaining := 0

		type result struct {
			idx      int
			inserted int
			failed   []json.RawMessage
		}
		results := make(chan result, len(allData))
		active := 0

		for i := range allData {
			cd := &allData[i]
			if len(cd.items) == 0 {
				continue
			}
			active++
			idx := i
			items := cd.items
			name := cd.name

			go func() {
				sem <- struct{}{}
				defer func() { <-sem }()

				ins, failed := batchInsert(client, name, items)
				progress.inserted.Add(int64(ins))
				results <- result{idx: idx, inserted: ins, failed: failed}
			}()
		}

		for range active {
			r := <-results
			cd := &allData[r.idx]
			passInserted += r.inserted

			if len(r.failed) > 0 {
				cd.items = r.failed
				if pass < maxPasses {
					passRemaining += len(r.failed)
				}
			} else {
				cd.items = nil
			}
		}

		log(fmt.Sprintf("Pass %d: %d inserted", pass, passInserted))

		if passRemaining == 0 {
			break
		}
	}

	// Count items that remain after all passes as failed.
	for _, cd := range allData {
		progress.failed.Add(int64(len(cd.items)))
	}

	return progress
}

func batchInsert(client *apiClient, collection string, items []json.RawMessage) (int, []json.RawMessage) {
	inserted := 0
	var retryable []json.RawMessage

	path := "/items/" + url.PathEscape(collection)
	chunkSize := client.BatchSize

	for i := 0; i < len(items); i += chunkSize {
		end := min(i+chunkSize, len(items))
		chunk := items[i:end]

		// Try the whole chunk in one POST. Directus rejects atomically on the
		// first FK violation, so on failure we retry one-by-one to isolate the
		// rows that are still blocked vs the rows that just got unlucky.
		batchData, _ := json.Marshal(chunk)
		_, status, _ := client.post(path, batchData)
		if status >= 200 && status < 300 {
			inserted += len(chunk)
			continue
		}

		// Per-item fallback. Items that still fail go back to the caller for
		// the next pass. "Already exists" responses count as success — they
		// happen when an earlier pass partially succeeded before a batch error.
		for _, item := range chunk {
			respBody, st, _ := client.post(path, item)
			switch {
			case st >= 200 && st < 300:
				inserted++
			case isAlreadyExists(string(respBody)):
				inserted++
			default:
				retryable = append(retryable, item)
			}
		}
	}
	return inserted, retryable
}

// Parallel data pull

func pullAllData(client *apiClient, collections []string, log func(string)) map[string][]json.RawMessage {
	return pullAllDataWithProgress(client, collections, func(col string, done, total int) {
		log(fmt.Sprintf("  %s: done (%d/%d)", col, done, total))
	})
}

// pullAllDataWithProgress fetches data for all collections in parallel.
// On HTTP errors it retries once after a short pause.
// Calls onProgress after each collection finishes.
func pullAllDataWithProgress(client *apiClient, collections []string, onProgress func(col string, done, total int)) map[string][]json.RawMessage {
	result := make(map[string][]json.RawMessage)
	var mu sync.Mutex
	total := len(collections)
	done := 0
	sem := make(chan struct{}, client.Concurrency)
	var wg sync.WaitGroup

	for _, col := range collections {
		wg.Add(1)
		col := col
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			items, err := client.pullAllItems(col)
			mu.Lock()
			if err != nil {
				// Rate limit: reduce concurrency on error, retry once after pause.
				mu.Unlock()
				time.Sleep(500 * time.Millisecond)
				items, err = client.pullAllItems(col)
				mu.Lock()
			}
			if err == nil {
				result[col] = items
			}
			done++
			d := done
			mu.Unlock()
			onProgress(col, d, total)
		}()
	}

	wg.Wait()
	return result
}
