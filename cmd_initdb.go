package main

import (
	"fmt"
	syslog "log"
	"os"
	"strings"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/go-redis/redis/v8"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"

	"github.com/rqzrqh/sync_filecoin/initdb"
	"github.com/rqzrqh/sync_filecoin/util"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var cmdInitDb = &cli.Command{
	Name:  "initdb",
	Usage: "Start initdb",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "node",
			Usage: "lotus fullnode rpc",
		},
		&cli.StringFlag{
			Name:  "db",
			Usage: "root:123456@tcp(127.0.0.1:3306)/lotus_chain",
		},
		&cli.StringFlag{
			Name:  "redis",
			Usage: "127.0.0.1:6379",
		},
		&cli.StringFlag{
			Name:  "height",
			Usage: "specific a start height",
			Value: "0",
		},
		&cli.StringFlag{
			Name:  "limit",
			Usage: "specific headchain limit size",
			Value: "900", // lotus is 900
		},
	},
	Action: func(cctx *cli.Context) error {

		ctx := util.ReqContext(cctx)

		go func() {
			<-ctx.Done()
			os.Exit(0)
		}()

		if err := logging.SetLogLevel("*", "debug"); err != nil {
			return err
		}
		if err := logging.SetLogLevel("rpc", "error"); err != nil {
			return err
		}

		// connect backend node
		tokenAddr := cctx.String("node")
		if tokenAddr == "" {
			return fmt.Errorf("no api info")
		}

		var node api.FullNode
		var closer jsonrpc.ClientCloser
		var err error

		{
			tos := strings.Split(tokenAddr, ":")
			if len(tos) != 2 {
				return fmt.Errorf("invalid api tokens, expected <token>:<maddr>, got: %s", tokenAddr)
			}

			node, closer, err = util.GetFullNodeAPIUsingCredentials(cctx.Context, tos[1], tos[0])
			if err != nil {
				return err
			}
		}
		defer closer()

		v, err := node.Version(ctx)
		if err != nil {
			return err
		}

		log.Infof("Remote version: %v", v.Version)

		newLogger := logger.New(
			syslog.New(os.Stdout, "\r\n", syslog.LstdFlags), // io writer（日志输出的目标，前缀和日志包含的内容——译者注）
			logger.Config{
				SlowThreshold:             1000 * time.Millisecond,
				LogLevel:                  logger.Warn,
				IgnoreRecordNotFoundError: true, // 忽略ErrRecordNotFound（记录未找到）错误
				Colorful:                  true,
			},
		)

		db, err := gorm.Open(mysql.Open(cctx.String("db")), &gorm.Config{
			Logger: newLogger,
		})
		if err != nil {
			fmt.Println("failed to connect database ", err)
			os.Exit(0)
		}

		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		if err := sqlDB.Ping(); err != nil {
			return err
		}
		log.Info("sql ping success")

		// redis
		/*
			sentinelAddrs := os.Getenv("ABC_REDIS_SENTINEL_ADDRS")
			sentinelMasterName := os.Getenv("ABC_REDIS_SENTINEL_MASTER")
			sentinelPassword := os.Getenv("ABC_REDIS_SENTINEL_PASSWORD")
			sentinelDB := 0
			if m, err := strconv.Atoi(cctx.String("redis-cache-db")); err == nil {
				if m >= 0 && m <= 15 {
					sentinelDB = m
				}
			}
			rdb := redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    sentinelMasterName,
				SentinelAddrs: strings.Split(sentinelAddrs, ","),
				Password:      sentinelPassword,
				DB:            sentinelDB,
			})
			defer rdb.Close()
		*/
		redis_addr := cctx.String("redis")
		rds := redis.NewClient(&redis.Options{
			Addr:     redis_addr,
			Password: "",
			DB:       0,
		})
		defer rds.Close()
		pong, err := rds.Ping(ctx).Result()
		if err != nil {
			return err
		}
		log.Info("redis response ", pong)

		if err := initdb.InitDatabase(ctx, db, rds, node, cctx.Int64("height"), cctx.Int("limit")); err != nil {
			log.Errorf("init failed. %s", err)
		} else {
			log.Info("init success.")
		}

		os.Exit(0)
		return nil
	},
}
