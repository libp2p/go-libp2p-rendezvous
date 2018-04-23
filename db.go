package rendezvous

import (
	"context"
	"errors"

	peer "github.com/libp2p/go-libp2p-peer"
)

type DB struct {
}

func OpenDB(ctx context.Context, path string) (*DB, error) {
	return nil, errors.New("IMPLEMENTME: OpenDB")
}

func (db *DB) Register(p peer.ID, ns string, addrs [][]byte, ttl int) error {
	return errors.New("IMPLEMENTME: DB.Register")
}

func (db *DB) CountRegistrations(p peer.ID) (int, error) {
	return 0, errors.New("IMPLEMENTME: DB.CountRegistrations")
}

func (db *DB) Unregister(p peer.ID, ns string) error {
	return errors.New("IMPLEMENTME: DB.Unregister")
}

func (db *DB) ValidCookie(ns string, cookie []byte) bool {
	return false
}

func (db *DB) Discover(ns string, cookie []byte, limit int) ([]RegistrationRecord, []byte, error) {
	return nil, nil, errors.New("IMPLEMENTME: DB.Discover")
}
