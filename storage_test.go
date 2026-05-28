package tests

import (
	"testing"
	"time"

	"github.com/Lenecplusultra/distributed-kv-store/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetAndGet(t *testing.T) {
	s := storage.New()
	s.Set("name", "alice")

	val, ok := s.Get("name")
	require.True(t, ok)
	assert.Equal(t, "alice", val)
}

func TestDelete(t *testing.T) {
	s := storage.New()
	s.Set("key", "value")

	deleted := s.Delete("key")
	assert.True(t, deleted)

	_, ok := s.Get("key")
	assert.False(t, ok)
}

func TestGetMissing(t *testing.T) {
	s := storage.New()
	_, ok := s.Get("nonexistent")
	assert.False(t, ok)
}

func TestTTLExpiry(t *testing.T) {
	s := storage.New()
	s.SetWithTTL("temp", "value", 50*time.Millisecond)

	val, ok := s.Get("temp")
	require.True(t, ok)
	assert.Equal(t, "value", val)

	time.Sleep(100 * time.Millisecond)

	_, ok = s.Get("temp")
	assert.False(t, ok, "key should have expired")
}
