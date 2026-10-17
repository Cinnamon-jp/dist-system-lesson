package store

import (
	"errors"
	"maps"
	"sync"
	"time"

	"dist_system/internal/model"
)

var (
	ErrStaleVersion = errors.New("stale version rejected")
	ErrKeyNotFound  = errors.New("key not found")
)

type Store struct {
	mu    sync.RWMutex
	items map[string]model.Item
}

func NewStore() *Store {
	return &Store{
		items: make(map[string]model.Item),
	}
}

// Get は指定したキーのアイテムを取得します
func (s *Store) Get(key string) (model.Item, bool) {
	s.mu.RLock() // 読み取りロック(他の読み取りは同時に行える。書き込みだけダメ)
	defer s.mu.RUnlock()

	val, ok := s.items[key]

	return val, ok
}

// Put はキーと値を保存します。
// version <= 0 の場合、Primaryとして自動的にインクリメントしたバージョンを付与します。
// version > 0 の場合、Backupとして指定されたバージョンでの更新を試みます（既存より古い場合はErrStaleVersion）。
func (s *Store) Put(key string, value string, version int64) (model.Item, error) {
	s.mu.Lock() // 全部ロック
	defer s.mu.Unlock()

	existing, exists := s.items[key]
	var newVersion int64

	if version <= 0 { // Primaryとしての動作
		if exists {
			newVersion = existing.Version + 1
		} else {
			newVersion = 1
		}
	} else { // Backupとしての動作
		if exists && version < existing.Version { // 既存のバージョンより古い場合はエラー
			return model.Item{}, ErrStaleVersion
		}
		newVersion = version // Primaryのバージョンを使用
	}

	newItem := model.Item{
		Key:       key,
		Value:     value,
		Version:   newVersion,
		UpdatedAt: time.Now(),
	}

	s.items[key] = newItem

	return newItem, nil
}

// Delete はキーを削除します
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exist := s.items[key]

	if !exist {
		return false
	}
	delete(s.items, key)

	return true
}

// GetAll は全アイテムのディープコピーを返します
func (s *Store) GetAll() map[string]model.Item {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return maps.Clone(s.items)
}

// Len は保持しているキーの総数を返します
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}
