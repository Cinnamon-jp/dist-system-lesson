package store

import (
	"errors"
	"sync"

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
	// TODO: Storeの初期化処理を実装してください
	return &Store{
		items: make(map[string]model.Item),
	}
}

// Get は指定したキーのアイテムを取得します
func (s *Store) Get(key string) (model.Item, bool) {
	// TODO: スレッドセーフにキーのアイテムを取得する処理を実装してください
	return model.Item{}, false
}

// Put はキーと値を保存します。
// version <= 0 の場合、Primaryとして自動的にインクリメントしたバージョンを付与します。
// version > 0 の場合、Backupとして指定されたバージョンでの更新を試みます（既存より古い場合はErrStaleVersion）。
func (s *Store) Put(key string, value string, version int64) (model.Item, error) {
	// TODO: バージョニングとスレッドセーフな書き込み処理を実装してください
	return model.Item{}, nil
}

// Delete はキーを削除します
func (s *Store) Delete(key string) bool {
	// TODO: キーの削除処理を実装してください
	return false
}

// GetAll は全アイテムのディープコピーを返します
func (s *Store) GetAll() map[string]model.Item {
	// TODO: 全アイテムのコピーを返す処理を実装してください
	return nil
}

// Len は保持しているキーの総数を返します
func (s *Store) Len() int {
	// TODO: 保持しているキーの総数を返す処理を実装してください
	return 0
}
