package store

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultConnectTimeout bounds opening one new connection to Postgres (TCP
// dial, TLS, startup and authentication together) when the DSN sets no
// connect_timeout.
//
// Without it, a black-holed database (packets dropped, nothing refused) makes
// a request wait for its whole 10s budget (api.requestTimeout) and end on its
// own context deadline, which is a 500, not a 503 (IsUnavailable ignores a
// request's deadline on purpose). With it, the connect fails first with a
// *pgconn.ConnectError, which IsUnavailable reports, so the client gets
// 503 unavailable in about this long.
//
// 2s, not more: a healthy Postgres opens a connection in milliseconds, even
// with TLS, so 2s only ever cuts off a database that is not answering. It is
// also below the 3s budget of an M1 lifecycle walk (api.lifecycleTimeout),
// so a walk that needs a new connection sees the connect error, not its own
// deadline. Not less: a loaded but working database must not be declared
// down.
const DefaultConnectTimeout = 2 * time.Second

// ParsePoolConfig parses a Postgres DSN (URL or key=value) into a pool config.
// If the DSN, or PGCONNECT_TIMEOUT, sets connect_timeout to a positive value,
// that value is kept; otherwise DefaultConnectTimeout applies.
// connect_timeout=0 means "wait forever" in libpq and pgx cannot tell it
// apart from "not set", so it also gets the default: an unbounded connect is
// exactly the failure this guards against.
func ParsePoolConfig(dsn string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// pgx's parse errors redact the password.
		return nil, fmt.Errorf("parse database config: %w", err)
	}
	if cfg.ConnConfig.ConnectTimeout <= 0 {
		cfg.ConnConfig.ConnectTimeout = DefaultConnectTimeout
	}
	return cfg, nil
}
