package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"geecache"
	"geecache/algo"
	"geecache/etcd/registry"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var identifierPattern = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

type mysqlConfig struct {
	Addr            string
	DBName          string
	User            string
	Password        string
	Table           string
	KeyColumn       string
	ValueColumn     string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func evictorFactory(name string) func() algo.Evictor {
	switch strings.ToLower(name) {
	case "lfu":
		return func() algo.Evictor { return algo.NewLFU() }
	case "lru-k", "lruk":
		return func() algo.Evictor { return algo.NewLRUK(2) }
	case "arc":
		return func() algo.Evictor { return algo.NewARC() }
	default:
		return func() algo.Evictor { return algo.NewLRU() }
	}
}

func (c mysqlConfig) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		c.User,
		c.Password,
		c.Addr,
		c.DBName,
	)
}

func (c mysqlConfig) lookupQuery() string {
	return fmt.Sprintf("SELECT `%s` FROM `%s` WHERE `%s` = ? LIMIT 1", c.ValueColumn, c.Table, c.KeyColumn)
}

func (c mysqlConfig) bloomKeysQuery() string {
	return fmt.Sprintf("SELECT `%s` FROM `%s`", c.KeyColumn, c.Table)
}

func openMySQL(cfg mysqlConfig) *sql.DB {
	validateMySQLConfig(cfg)

	db, err := sql.Open("mysql", cfg.dsn())
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		log.Fatalf("ping mysql: %v", err)
	}
	return db
}

func validateMySQLConfig(cfg mysqlConfig) {
	for label, value := range map[string]string{
		"database":     cfg.DBName,
		"table":        cfg.Table,
		"key column":   cfg.KeyColumn,
		"value column": cfg.ValueColumn,
	} {
		if !identifierPattern.MatchString(value) {
			log.Fatalf("invalid mysql %s %q", label, value)
		}
	}
}

func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		value = strings.TrimSpace(value)
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
	return scanner.Err()
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func loadScore(ctx context.Context, db *sql.DB, cfg mysqlConfig, key string) ([]byte, error) {
	log.Println("[MySQL] search key", key)

	var value string
	err := db.QueryRowContext(ctx, cfg.lookupQuery(), key).Scan(&value)
	if err == nil {
		return []byte(value), nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, geecache.ErrNotFound
	}
	return nil, err
}

func preloadBloomKeys(ctx context.Context, db *sql.DB, cfg mysqlConfig) ([]string, error) {
	rows, err := db.QueryContext(ctx, cfg.bloomKeysQuery())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

func createGroup(sourceDB *sql.DB, mysqlCfg mysqlConfig, emptyTTL time.Duration, peerRetries int, evictor string, bloomItems int, bloomFP float64, bloomRejectOnMiss bool) *geecache.Group {
	opts := []geecache.Option{
		geecache.WithCacheTTL(2*time.Minute, 15*time.Second),
		geecache.WithEmptyCache(emptyTTL),
		geecache.WithPeerRetries(peerRetries),
		geecache.WithPeerRetryBackoff(100 * time.Millisecond),
		geecache.WithPeerCircuitBreaker(3, 3*time.Second),
		geecache.WithEvictor(evictorFactory(evictor)),
	}
	if bloomItems > 0 {
		filter, err := geecache.NewBloomFilter(bloomItems, bloomFP)
		if err != nil {
			log.Fatalf("create bloom filter: %v", err)
		}
		opts = append(opts, geecache.WithBloomFilter(filter))
		if bloomRejectOnMiss {
			opts = append(opts, geecache.WithBloomRejectOnMiss())
		}
	}

	group := geecache.NewGroupWithOptions(
		"scores",
		2<<10,
		geecache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return loadScore(ctx, sourceDB, mysqlCfg, key)
		}),
		opts...,
	)
	if bloomItems > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		keys, err := preloadBloomKeys(ctx, sourceDB, mysqlCfg)
		if err != nil {
			log.Fatalf("preload bloom keys: %v", err)
		}
		group.AddBloomKeys(keys...)
	}
	return group
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	peers := make([]string, 0, len(parts))
	for _, peer := range parts {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func main() {
	if err := loadDotEnv(".env"); err != nil {
		log.Fatalf("load .env: %v", err)
	}

	var (
		addr                  string
		peers                 string
		useEtcd               bool
		etcd                  string
		serviceName           string
		weight                int
		api                   bool
		apiAddr               string
		emptyTTL              time.Duration
		peerRetries           int
		evictor               string
		bloomItems            int
		bloomFalsePositive    float64
		bloomRejectOnMiss     bool
		mysqlAddr             string
		mysqlDB               string
		mysqlUser             string
		mysqlPassword         string
		mysqlTable            string
		mysqlKeyColumn        string
		mysqlValueColumn      string
		mysqlMaxOpenConns     int
		mysqlMaxIdleConns     int
		mysqlConnMaxLifetime  time.Duration
		mysqlConnMaxIdleTime  time.Duration
		tlsCert               string
		tlsKey                string
		tlsCA                 string
		tlsServerName         string
		tlsInsecureSkipVerify bool
	)

	flag.StringVar(&addr, "addr", "localhost:8001", "Current cache node address")
	flag.StringVar(&peers, "peers", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated cache peers")
	flag.BoolVar(&useEtcd, "use-etcd", false, "Use etcd service discovery instead of the static peer list")
	flag.StringVar(&etcd, "etcd-endpoints", "localhost:2379", "Comma-separated etcd endpoints")
	flag.StringVar(&serviceName, "service-name", registry.DefaultServiceName, "Service name used for etcd registration/discovery")
	flag.IntVar(&weight, "weight", 1, "Node routing weight used by etcd discovery; static peers can also use addr@weight syntax")
	flag.BoolVar(&api, "api", false, "Start an API server")
	flag.StringVar(&apiAddr, "api-addr", "localhost:9999", "API server listen address")
	flag.DurationVar(&emptyTTL, "empty-ttl", 30*time.Second, "TTL for not-found cache entries")
	flag.IntVar(&peerRetries, "peer-retries", 1, "Number of peer retries before local fallback")
	flag.StringVar(&evictor, "evictor", "lru", "Cache eviction algorithm: lru, lfu, lru-k, or arc")
	flag.IntVar(&bloomItems, "bloom-items", 0, "Expected item count for the optional bloom filter; 0 disables it")
	flag.Float64Var(&bloomFalsePositive, "bloom-fp-rate", 0.01, "Target false-positive rate for the optional bloom filter")
	flag.BoolVar(&bloomRejectOnMiss, "bloom-reject-on-miss", false, "Reject keys that are definitely absent from the bloom filter")
	flag.StringVar(&mysqlAddr, "mysql-addr", envOrDefault("MYSQL_ADDR", "127.0.0.1:3306"), "MySQL address")
	flag.StringVar(&mysqlDB, "mysql-db", envOrDefault("MYSQL_DB", "geecache"), "MySQL database name")
	flag.StringVar(&mysqlUser, "mysql-user", envOrDefault("MYSQL_USER", ""), "MySQL username")
	flag.StringVar(&mysqlPassword, "mysql-password", envOrDefault("MYSQL_PASSWORD", ""), "MySQL password")
	flag.StringVar(&mysqlTable, "mysql-table", envOrDefault("MYSQL_TABLE", "scores"), "MySQL source table name")
	flag.StringVar(&mysqlKeyColumn, "mysql-key-column", envOrDefault("MYSQL_KEY_COLUMN", "name"), "MySQL source key column")
	flag.StringVar(&mysqlValueColumn, "mysql-value-column", envOrDefault("MYSQL_VALUE_COLUMN", "score"), "MySQL source value column")
	flag.IntVar(&mysqlMaxOpenConns, "mysql-max-open-conns", envIntOrDefault("MYSQL_MAX_OPEN_CONNS", 10), "MySQL max open connections")
	flag.IntVar(&mysqlMaxIdleConns, "mysql-max-idle-conns", envIntOrDefault("MYSQL_MAX_IDLE_CONNS", 5), "MySQL max idle connections")
	flag.DurationVar(&mysqlConnMaxLifetime, "mysql-conn-max-lifetime", envDurationOrDefault("MYSQL_CONN_MAX_LIFETIME", 30*time.Minute), "MySQL connection max lifetime")
	flag.DurationVar(&mysqlConnMaxIdleTime, "mysql-conn-max-idle-time", envDurationOrDefault("MYSQL_CONN_MAX_IDLE_TIME", 5*time.Minute), "MySQL connection max idle time")
	flag.StringVar(&tlsCert, "tls-cert", "", "TLS certificate file for gRPC server and peer client auth")
	flag.StringVar(&tlsKey, "tls-key", "", "TLS private key file for gRPC server and peer client auth")
	flag.StringVar(&tlsCA, "tls-ca", "", "Optional CA file used to verify peer certificates")
	flag.StringVar(&tlsServerName, "tls-server-name", "", "Optional TLS server name override for peer dialing")
	flag.BoolVar(&tlsInsecureSkipVerify, "tls-insecure-skip-verify", false, "Skip peer certificate verification")
	flag.Parse()

	mysqlCfg := mysqlConfig{
		Addr:            mysqlAddr,
		DBName:          mysqlDB,
		User:            mysqlUser,
		Password:        mysqlPassword,
		Table:           mysqlTable,
		KeyColumn:       mysqlKeyColumn,
		ValueColumn:     mysqlValueColumn,
		MaxOpenConns:    mysqlMaxOpenConns,
		MaxIdleConns:    mysqlMaxIdleConns,
		ConnMaxLifetime: mysqlConnMaxLifetime,
		ConnMaxIdleTime: mysqlConnMaxIdleTime,
	}
	sourceDB := openMySQL(mysqlCfg)
	defer sourceDB.Close()

	group := createGroup(sourceDB, mysqlCfg, emptyTTL, peerRetries, evictor, bloomItems, bloomFalsePositive, bloomRejectOnMiss)
	groupMgr, err := geecache.NewGroupManager(group)
	if err != nil {
		log.Fatalf("create group manager: %v", err)
	}

	var tlsOpts *geecache.TLSOptions
	if tlsCert != "" || tlsKey != "" || tlsCA != "" || tlsServerName != "" || tlsInsecureSkipVerify {
		tlsOpts = &geecache.TLSOptions{
			CertFile:           tlsCert,
			KeyFile:            tlsKey,
			CAFile:             tlsCA,
			ServerName:         tlsServerName,
			InsecureSkipVerify: tlsInsecureSkipVerify,
		}
	}

	server, err := geecache.NewServer(geecache.ServerOptions{
		Addr:        addr,
		EnableAPI:   api,
		APIAddr:     apiAddr,
		UseEtcd:     useEtcd,
		StaticPeers: splitList(peers),
		ServiceName: serviceName,
		Registry: registry.Config{
			Endpoints:   splitList(etcd),
			ServiceName: serviceName,
			Weight:      weight,
		},
		Groups: groupMgr,
		TLS:    tlsOpts,
	})
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if useEtcd {
		fmt.Printf("node=%s discovery=etcd etcd=%v service=%s weight=%d mysql=%s/%s.%s emptyTTL=%s peerRetries=%d evictor=%s bloomItems=%d bloomRejectOnMiss=%t\n",
			addr, splitList(etcd), serviceName, weight, mysqlDB, mysqlTable, mysqlValueColumn, emptyTTL, peerRetries, evictor, bloomItems, bloomRejectOnMiss)
	} else {
		fmt.Printf("node=%s peers=%v mysql=%s/%s.%s emptyTTL=%s peerRetries=%d evictor=%s bloomItems=%d bloomRejectOnMiss=%t\n",
			addr, splitList(peers), mysqlDB, mysqlTable, mysqlValueColumn, emptyTTL, peerRetries, evictor, bloomItems, bloomRejectOnMiss)
	}
	if err := server.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("server stopped: %v", err)
	}
}
