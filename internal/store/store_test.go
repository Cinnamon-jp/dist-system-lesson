package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestStore_BasicPutAndGet(t *testing.T) {
	s := NewStore()

	// Initial Get should return not found
	if _, ok := s.Get("hello"); ok {
		t.Fatalf("expected key 'hello' not to exist")
	}

	// Put new key (version <= 0 for auto-versioning)
	item, err := s.Put("hello", "world", 0)
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}
	if item.Version != 1 {
		t.Fatalf("expected version 1, got %d", item.Version)
	}
	if item.Value != "world" {
		t.Fatalf("expected value 'world', got %s", item.Value)
	}

	// Get key
	got, ok := s.Get("hello")
	if !ok {
		t.Fatalf("expected key 'hello' to exist")
	}
	if got.Value != "world" || got.Version != 1 {
		t.Fatalf("unexpected got item: %+v", got)
	}

	// Update existing key
	updated, err := s.Put("hello", "world2", 0)
	if err != nil {
		t.Fatalf("unexpected error on update: %v", err)
	}
	if updated.Version != 2 {
		t.Fatalf("expected version 2, got %d", updated.Version)
	}
}

func TestStore_ReplicationVersionControl(t *testing.T) {
	s := NewStore()

	// Replicate explicit version 5
	item, err := s.Put("k1", "v5", 5)
	if err != nil {
		t.Fatalf("failed to put explicit version: %v", err)
	}
	if item.Version != 5 {
		t.Fatalf("expected version 5, got %d", item.Version)
	}

	// Replicate stale version 3 (should fail)
	_, err = s.Put("k1", "v3", 3)
	if err != ErrStaleVersion {
		t.Fatalf("expected ErrStaleVersion, got %v", err)
	}

	// Replicate equal or higher version 6 (should succeed)
	item6, err := s.Put("k1", "v6", 6)
	if err != nil {
		t.Fatalf("unexpected error updating to higher version: %v", err)
	}
	if item6.Version != 6 || item6.Value != "v6" {
		t.Fatalf("unexpected item: %+v", item6)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup

	numGoroutines := 50
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d", j%10)
				val := fmt.Sprintf("val-%d-%d", id, j)
				_, _ = s.Put(key, val, 0)
				_, _ = s.Get(key)
			}
		}(i)
	}

	wg.Wait()

	if s.Len() != 10 {
		t.Fatalf("expected 10 keys, got %d", s.Len())
	}
}
