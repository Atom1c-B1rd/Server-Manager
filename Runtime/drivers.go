package scriptrt

// Database drivers available to sdk.DB. database/sql drivers must be compiled
// into the binary, so the list of engines a server can connect to is whatever
// is imported here. The UI offers exactly the drivers registered at runtime.
//
// To add another engine, add a blank import and run `go mod tidy`, e.g. for
// SQLite without CGO:
//
//	_ "modernc.org/sqlite" // driver name: "sqlite"
import (
	_ "github.com/go-sql-driver/mysql" // driver name: "mysql"
	_ "github.com/lib/pq"              // driver name: "postgres"
)
