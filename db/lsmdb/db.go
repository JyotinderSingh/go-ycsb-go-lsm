package lsmdb

import (
	"context"
	"fmt"
	"os"

	"github.com/JyotinderSingh/golsm"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	golsmDir = "golsm.dir"
)

type lsmdbCreator struct {
}

type lsmdb struct {
	ycsb.DB
	db      *golsm.LSMTree
	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c lsmdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(golsmDir, "/tmp/golsm")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dir)
	}

	db, err := golsm.Open(dir, 32_000_000, false)
	if err != nil {
		return nil, err
	}
	return &lsmdb{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func (db *lsmdb) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *lsmdb) CleanupThread(ctx context.Context) {
}

func (db *lsmdb) Close() error {
	db.db.Close()
	return nil
}

func (db *lsmdb) getRowKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

func (db *lsmdb) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	return db.db.Put(rowKey, buf)
}

func (db *lsmdb) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)

	return db.db.Delete(rowKey)
}

func (db *lsmdb) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err = db.r.Encode(buf, m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)

	return db.db.Put(rowKey, buf)
}

func (db *lsmdb) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get(db.getRowKey(table, key))
	if err != nil {
		return nil, err
	}

	return db.r.Decode(value, fields)
}

func init() {
	ycsb.RegisterDBCreator("lsmdb", lsmdbCreator{})
}
