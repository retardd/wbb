package pgxRepository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const writeLimit = 1000
const timeout = 30 * time.Second

type PGXRepository struct {
	db             *pgxpool.Pool
	dbSlave        *pgxpool.Pool
	externalMainDB *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool, dbSlave *pgxpool.Pool, externalApiDB *pgxpool.Pool) *PGXRepository {
	return &PGXRepository{
		db:             db,
		dbSlave:        dbSlave,
		externalMainDB: externalApiDB,
	}
}

func (r *PGXRepository) Ping(ctx context.Context) error {
	return r.db.Ping(ctx)
}

func (r *PGXRepository) MultiInsert(ctx context.Context, pool *pgxpool.Pool, query string, list [][]interface{}, uniqN int, addData ...string) (err error) {
	if len(list) == 0 {
		return
	}
	valueCount := len(list[0])
	if valueCount == 0 {
		return
	}

	valueStrings := make([]string, 0, writeLimit)
	valueArgs := make([]interface{}, 0, valueCount*writeLimit)

	uniq := make(map[interface{}]bool)
	for idx := range list {
		if uniqN > 0 {
			if uniq[list[idx][uniqN-1]] {
				continue
			}
			uniq[list[idx][uniqN-1]] = true
		}

		value := "("
		values := make([]string, 0, valueCount)
		for k := 0; k < valueCount; k++ {
			values = append(values, fmt.Sprintf("$%d", len(valueStrings)*valueCount+k+1))
			valueArgs = append(valueArgs, list[idx][k])
		}
		values = append(values, addData...)
		value += strings.Join(values, ",")
		value += ")"
		valueStrings = append(valueStrings, value)

		// если упираемся в лимит записи то отправляем данные в бд и собираем пулл заного
		if len(valueStrings) >= writeLimit {
			err := r.ExecBatch(ctx, pool, query, valueStrings, valueArgs)
			if err != nil {
				return err
			}
			valueStrings = make([]string, 0, writeLimit)
			valueArgs = make([]interface{}, 0, valueCount*writeLimit)
			continue
		}

		// если записей меньше чем у нас лимит то продолжаем набирать их пока не дойдем до конца списка
		if idx+1 == len(list) {
			err := r.ExecBatch(ctx, pool, query, valueStrings, valueArgs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *PGXRepository) ExecBatch(ctx context.Context, pool *pgxpool.Pool, queryString string, vv []string, v []interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	queryString = fmt.Sprintf(queryString, strings.Join(vv, ","))
	_, err := pool.Exec(ctx, queryString, v...)
	return err
}
