package db

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	dbi "github.com/libp2p/go-libp2p-rendezvous/db"

	_ "github.com/mattn/go-sqlite3"

	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("rendezvous/db")

type DB struct {
	db *sql.DB

	insertPeerRegistration     *sql.Stmt
	deletePeerRegistrations    *sql.Stmt
	deletePeerRegistrationsNs  *sql.Stmt
	countPeerRegistrations     *sql.Stmt
	selectPeerRegistrations    *sql.Stmt
	selectPeerRegistrationsNS  *sql.Stmt
	selectPeerRegistrationsC   *sql.Stmt
	selectPeerRegistrationsNSC *sql.Stmt
	deleteExpiredRegistrations *sql.Stmt

	nonce []byte

	cancel func()
}

func OpenDB(ctx context.Context, path string) (*DB, error) {
	var create bool
	if path == ":memory:" {
		create = true
	} else {
		_, err := os.Stat(path)
		switch {
		case os.IsNotExist(err):
			create = true
		case err != nil:
			return nil, err
		}
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	if path == ":memory:" {
		// this is necessary to avoid creating a new database on each connection
		db.SetMaxOpenConns(1)
	}

	rdb := &DB{db: db}
	if create {
		err = rdb.prepareDB()
		if err != nil {
			db.Close()
			return nil, err
		}
	} else {
		err = rdb.loadNonce()
		if err != nil {
			db.Close()
			return nil, err
		}
	}

	err = rdb.prepareStmts()
	if err != nil {
		db.Close()
		return nil, err
	}

	bgctx, cancel := context.WithCancel(ctx)
	rdb.cancel = cancel
	go rdb.background(bgctx)

	return rdb, nil
}

func (db *DB) Close() error {
	db.cancel()
	return db.db.Close()
}

func (db *DB) prepareDB() error {
	_, err := db.db.Exec("CREATE TABLE Registrations (counter INTEGER PRIMARY KEY AUTOINCREMENT, peer VARCHAR(64), ns VARCHAR, expire INTEGER, addrs VARBINARY)")
	if err != nil {
		return err
	}

	_, err = db.db.Exec("CREATE TABLE Nonce (nonce VARBINARY)")
	if err != nil {
		return err
	}

	nonce := make([]byte, 32)
	_, err = rand.Read(nonce)
	if err != nil {
		return err
	}

	_, err = db.db.Exec("INSERT INTO Nonce VALUES (?)", nonce)
	if err != nil {
		return err
	}

	db.nonce = nonce
	return nil
}

func (db *DB) loadNonce() error {
	var nonce []byte
	row := db.db.QueryRow("SELECT nonce FROM Nonce")
	err := row.Scan(&nonce)
	if err != nil {
		return err
	}

	db.nonce = nonce
	return nil
}

func (db *DB) prepareStmts() error {
	stmt, err := db.db.Prepare("INSERT INTO Registrations VALUES (NULL, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	db.insertPeerRegistration = stmt

	stmt, err = db.db.Prepare("DELETE FROM Registrations WHERE peer = ?")
	if err != nil {
		return err
	}
	db.deletePeerRegistrations = stmt

	stmt, err = db.db.Prepare("DELETE FROM Registrations WHERE peer = ? AND ns = ?")
	if err != nil {
		return err
	}
	db.deletePeerRegistrationsNs = stmt

	stmt, err = db.db.Prepare("SELECT COUNT(*) FROM Registrations WHERE peer = ?")
	if err != nil {
		return err
	}
	db.countPeerRegistrations = stmt

	stmt, err = db.db.Prepare("SELECT * FROM Registrations WHERE expire > ? LIMIT ?")
	if err != nil {
		return err
	}
	db.selectPeerRegistrations = stmt

	stmt, err = db.db.Prepare("SELECT * FROM Registrations WHERE ns = ? AND expire > ? LIMIT ?")
	if err != nil {
		return err
	}
	db.selectPeerRegistrationsNS = stmt

	stmt, err = db.db.Prepare("SELECT * FROM Registrations WHERE counter > ? AND expire > ? LIMIT ?")
	if err != nil {
		return err
	}
	db.selectPeerRegistrationsC = stmt

	stmt, err = db.db.Prepare("SELECT * FROM Registrations WHERE counter > ? AND ns = ? AND expire > ? LIMIT ?")
	if err != nil {
		return err
	}
	db.selectPeerRegistrationsNSC = stmt

	stmt, err = db.db.Prepare("DELETE FROM Registrations WHERE expire < ?")
	if err != nil {
		return err
	}
	db.deleteExpiredRegistrations = stmt

	return nil
}

func (db *DB) Register(p peer.ID, ns string, addrs [][]byte, ttl int) error {
	pid := p.Pretty()
	maddrs := packAddrs(addrs)
	expire := time.Now().Unix() + int64(ttl)

	tx, err := db.db.Begin()
	if err != nil {
		return err
	}

	delOld := tx.Stmt(db.deletePeerRegistrationsNs)
	insertNew := tx.Stmt(db.insertPeerRegistration)

	_, err = delOld.Exec(pid, ns)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = insertNew.Exec(pid, ns, expire, maddrs)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (db *DB) CountRegistrations(p peer.ID) (int, error) {
	pid := p.Pretty()

	row := db.countPeerRegistrations.QueryRow(pid)

	var count int
	err := row.Scan(&count)

	return count, err
}

func (db *DB) Unregister(p peer.ID, ns string) error {
	pid := p.Pretty()

	var err error

	if ns == "" {
		_, err = db.deletePeerRegistrations.Exec(pid)
	} else {
		_, err = db.deletePeerRegistrationsNs.Exec(pid, ns)
	}

	return err
}

func (db *DB) Discover(ns string, cookie []byte, limit int) ([]dbi.RegistrationRecord, []byte, error) {
	now := time.Now().Unix()

	var (
		counter int64
		rows    *sql.Rows
		err     error
	)

	if cookie != nil {
		counter, err = unpackCookie(cookie)
		if err != nil {
			log.Errorf("error unpacking cookie: %s", err.Error())
			return nil, nil, err
		}
	}

	if counter > 0 {
		if ns == "" {
			rows, err = db.selectPeerRegistrationsC.Query(counter, now, limit)
		} else {
			rows, err = db.selectPeerRegistrationsNSC.Query(counter, ns, now, limit)
		}
	} else {
		if ns == "" {
			rows, err = db.selectPeerRegistrations.Query(now, limit)
		} else {
			rows, err = db.selectPeerRegistrationsNS.Query(ns, now, limit)
		}
	}

	if err != nil {
		log.Errorf("query error: %s", err.Error())
		return nil, nil, err
	}

	defer rows.Close()

	regs := make([]dbi.RegistrationRecord, 0, limit)
	for rows.Next() {
		var (
			reg    dbi.RegistrationRecord
			rid    string
			rns    string
			expire int64
			raddrs []byte
			addrs  [][]byte
			p      peer.ID
		)

		err = rows.Scan(&counter, &rid, &rns, &expire, &raddrs)
		if err != nil {
			log.Errorf("row scan error: %s", err.Error())
			return nil, nil, err
		}

		p, err = peer.IDB58Decode(rid)
		if err != nil {
			log.Errorf("error decoding peer id: %s", err.Error())
			continue
		}

		addrs, err := unpackAddrs(raddrs)
		if err != nil {
			log.Errorf("error unpacking address: %s", err.Error())
			continue
		}

		reg.Id = p
		reg.Addrs = addrs
		reg.Ttl = int(expire - now)

		if ns == "" {
			reg.Ns = rns
		}

		regs = append(regs, reg)
	}

	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}

	if counter > 0 {
		cookie = packCookie(counter, ns, db.nonce)
	}

	return regs, cookie, nil
}

func (db *DB) ValidCookie(ns string, cookie []byte) bool {
	return validCookie(cookie, ns, db.nonce)
}

func (db *DB) background(ctx context.Context) {
	for {
		db.cleanupExpired()

		select {
		case <-time.After(15 * time.Minute):
		case <-ctx.Done():
			return
		}
	}
}

func (db *DB) cleanupExpired() {
	now := time.Now().Unix()
	_, err := db.deleteExpiredRegistrations.Exec(now)
	if err != nil {
		log.Errorf("error deleting expired registrations: %s", err.Error())
	}
}

func packAddrs(addrs [][]byte) []byte {
	packlen := 0
	for _, addr := range addrs {
		packlen = packlen + 2 + len(addr)
	}

	packed := make([]byte, packlen)
	buf := packed
	for _, addr := range addrs {
		binary.BigEndian.PutUint16(buf, uint16(len(addr)))
		buf = buf[2:]
		copy(buf, addr)
		buf = buf[len(addr):]
	}

	return packed
}

func unpackAddrs(packed []byte) ([][]byte, error) {
	var addrs [][]byte

	buf := packed
	for len(buf) > 1 {
		l := binary.BigEndian.Uint16(buf)
		buf = buf[2:]
		if len(buf) < int(l) {
			return nil, fmt.Errorf("bad packed address: not enough bytes %v %v", packed, buf)
		}
		addr := make([]byte, l)
		copy(addr, buf[:l])
		buf = buf[l:]
		addrs = append(addrs, addr)
	}

	if len(buf) > 0 {
		return nil, fmt.Errorf("bad packed address: unprocessed bytes: %v %v", packed, buf)
	}

	return addrs, nil
}

// cookie: counter:SHA256(nonce + ns + counter)
func packCookie(counter int64, ns string, nonce []byte) []byte {
	cbits := make([]byte, 8)
	binary.BigEndian.PutUint64(cbits, uint64(counter))

	hash := sha256.New()
	_, err := hash.Write(nonce)
	if err != nil {
		panic(err)
	}
	_, err = hash.Write([]byte(ns))
	if err != nil {
		panic(err)
	}
	_, err = hash.Write(cbits)
	if err != nil {
		panic(err)
	}

	return hash.Sum(cbits)
}

func unpackCookie(cookie []byte) (int64, error) {
	if len(cookie) < 8 {
		return 0, fmt.Errorf("bad packed cookie: not enough bytes: %v", cookie)
	}

	counter := binary.BigEndian.Uint64(cookie[:8])
	return int64(counter), nil
}

func validCookie(cookie []byte, ns string, nonce []byte) bool {
	if len(cookie) != 40 {
		return false
	}

	cbits := cookie[:8]
	hash := sha256.New()
	_, err := hash.Write(nonce)
	if err != nil {
		panic(err)
	}
	_, err = hash.Write([]byte(ns))
	if err != nil {
		panic(err)
	}
	_, err = hash.Write(cbits)
	if err != nil {
		panic(err)
	}
	hbits := hash.Sum(nil)

	return bytes.Equal(cookie[8:], hbits)
}
