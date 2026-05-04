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
//
// Ordering: collections are processed sequentially in `order` (FK-safe topo
// sort). Parallelism lives INSIDE each collection — chunks fan out across
// `Concurrency` workers. We tried flattening (collection, chunk) into one
// global queue; the gain on huge tables was real, but chunks of a child
// collection ran ahead of the parent collection's last chunks, blowing up
// FK conflicts and pushing twice as much work into pass 2. Sequential
// collections keep that ordering intact while still saturating the workers
// for any collection big enough to matter.
func applyData(client *apiClient, order []string, dataMap map[string][]json.RawMessage,
	aliasFields map[string]map[string]bool, log func(string)) *dataProgress {
	progress := &dataProgress{}
	for _, items := range dataMap {
		progress.total += len(items)
	}

	// Prepare items per-collection: strip alias fields, fix datetimes.
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

	for pass := 1; pass <= maxPasses; pass++ {
		progress.pass = pass
		passInserted := 0
		passRemaining := 0

		// Walk collections in topological order. Within each collection,
		// batchInsert fans out chunks in parallel.
		for i := range allData {
			cd := &allData[i]
			if len(cd.items) == 0 {
				continue
			}
			ins, failed := batchInsert(client, cd.name, cd.items)
			progress.inserted.Add(int64(ins))
			passInserted += ins

			if len(failed) > 0 {
				cd.items = failed
				if pass < maxPasses {
					passRemaining += len(failed)
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

	for _, cd := range allData {
		progress.failed.Add(int64(len(cd.items)))
	}

	return progress
}

// batchInsert POSTs items for one collection in parallel chunks.
//
// Why fan out within a collection: a 50k-item translations table at
// chunkSize=100 is 500 sequential POSTs. Even at 50ms per round-trip that's
// 25 seconds for one collection while the other workers idle. Splitting
// chunks across `Concurrency` workers reclaims that idle time.
//
// Why fan out is safe: rows inside a single Directus collection have no
// inter-row constraints we care about — each item has its own PK and any
// FK references either point at other (already-inserted) collections or
// loop back to the same collection but always to existing rows. Order
// within a chunk doesn't matter; order across chunks doesn't matter.
func batchInsert(client *apiClient, collection string, items []json.RawMessage) (int, []json.RawMessage) {
	chunkSize := client.BatchSize
	if chunkSize < 1 {
		chunkSize = 100
	}

	// Slice into chunks up front so each runParallel job is independent.
	var chunks [][]json.RawMessage
	for i := 0; i < len(items); i += chunkSize {
		end := min(i+chunkSize, len(items))
		chunks = append(chunks, items[i:end])
	}

	var inserted atomic.Int64
	var retryMu sync.Mutex
	var retryable []json.RawMessage

	_ = runParallel(client, chunks, func(chunk []json.RawMessage) error {
		ins, fail := postChunk(client, collection, chunk)
		inserted.Add(int64(ins))
		if len(fail) > 0 {
			retryMu.Lock()
			retryable = append(retryable, fail...)
			retryMu.Unlock()
		}
		return nil
	})
	return int(inserted.Load()), retryable
}

// postChunk POSTs one batch of items to /items/<collection>. On a successful
// batch we're done. On any 4xx/5xx we retry items one-by-one to isolate the
// row(s) that are still blocked from the rows that just got rolled back
// alongside them — Directus batch inserts are atomic, so a single bad FK
// kills an otherwise-clean chunk.
func postChunk(client *apiClient, collection string, items []json.RawMessage) (int, []json.RawMessage) {
	path := "/items/" + url.PathEscape(collection)

	batchData, _ := json.Marshal(items)
	_, status, _ := client.post(path, batchData)
	if status >= 200 && status < 300 {
		return len(items), nil
	}

	// Per-item fallback. "Already exists" counts as success — it happens
	// when an earlier pass partially landed before a batch error rolled back
	// only the trailing rows.
	inserted := 0
	var retryable []json.RawMessage
	for _, item := range items {
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
