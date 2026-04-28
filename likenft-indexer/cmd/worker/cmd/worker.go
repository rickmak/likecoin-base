package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	verbosityutil "likenft-indexer/cmd/worker/cmd/util/verbosity"
	workercontext "likenft-indexer/cmd/worker/context"
	"likenft-indexer/cmd/worker/task"
	"likenft-indexer/internal/database"
	"likenft-indexer/internal/util/sentry"
	"likenft-indexer/internal/worker/middleware"

	"github.com/go-redis/redis"
	"github.com/hibiken/asynq"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var DEFAULT_QUEUE_PRIORITY = 1

var workerCmd = &cobra.Command{
	Use:   fmt.Sprintf("worker [%s]...", strings.Join(task.Tasks.GetRegisteredTasks(), " | ")),
	Short: "Start worker",
	Run: func(cmd *cobra.Command, args []string) {
		concurrency, err := cmd.Flags().GetInt("concurrency")
		if err != nil {
			_ = cmd.Usage()
			return
		}
		logger, err := verbosityutil.GetLoggerFromCmd(cmd)

		if len(args) < 1 {
			_ = cmd.Usage()
			return
		}

		queues := make(map[string]int)

		for _, q := range args {
			queues[q] = DEFAULT_QUEUE_PRIORITY
		}

		envCfg := workercontext.ConfigFromContext(cmd.Context())
		asynqClient := workercontext.AsynqClientFromContext(cmd.Context())
		asynqInspector := workercontext.AsynqInspectorFromContext(cmd.Context())
		evmQueryClient := workercontext.EvmQueryClientFromContext(cmd.Context())
		evmClient := workercontext.EvmClientFromContext(cmd.Context())

		hub, err := sentry.NewHub(envCfg.SentryDsn, envCfg.SentryDebug)

		if err != nil {
			panic(err)
		}

		opt, err := redis.ParseURL(envCfg.RedisDsn)
		if err != nil {
			panic(err)
		}
		redisClientOpt := asynq.RedisClientOpt{
			Network:      opt.Network,
			Addr:         opt.Addr,
			Password:     opt.Password,
			DB:           opt.DB,
			DialTimeout:  opt.DialTimeout,
			ReadTimeout:  opt.ReadTimeout,
			WriteTimeout: opt.WriteTimeout,
			PoolSize:     opt.PoolSize,
			TLSConfig:    opt.TLSConfig,
		}

		srv := asynq.NewServer(
			redisClientOpt,
			asynq.Config{
				Concurrency: concurrency,
				Queues:      queues,
			},
		)

		// mux maps a type to a handler
		mux := asynq.NewServeMux()

		worker, err := task.Tasks.MakeWorker(args...)
		if err != nil {
			panic(err)
		}

		worker.ConfigServerMux(mux)

		// ...register other handlers...
		mux.Use(workercontext.AsynqMiddlewareWithConfigContext(envCfg))
		mux.Use(workercontext.AsynqMiddlewareWithLoggerContext(logger))
		mux.Use(workercontext.AsynqMiddlewareWithAsynqClientContext(asynqClient))
		mux.Use(workercontext.AsynqMiddlewareWithAsynqInspectorContext(asynqInspector))
		mux.Use(workercontext.AsynqMiddlewareWithEvmQueryClientContext(evmQueryClient))
		mux.Use(workercontext.AsynqMiddlewareWithEvmClientContext(evmClient))
		mux.Use(middleware.MakeSentryMiddleware(hub).Handle)

		dbService := database.New()
		go runOwnerCountRefresh(cmd.Context(), dbService)

		if err := srv.Run(mux); err != nil {
			log.Fatalf("could not run server: %v", err)
		}
	},
}

// runOwnerCountRefresh refreshes owner-count materialized views on a ticker.
// Interval defaults to 10 minutes, overridable via OWNER_COUNT_REFRESH_INTERVAL (Go duration, e.g. "5m").
func runOwnerCountRefresh(ctx context.Context, dbSvc database.Service) {
	interval := 10 * time.Minute
	if v := os.Getenv("OWNER_COUNT_REFRESH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := dbSvc.RefreshOwnerCountViews(ctx); err != nil {
				log.Printf("owner count refresh failed: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func init() {
	_ = workerCmd.Flags().Int("concurrency", 1, "Worker concurrency")
	rootCmd.AddCommand(workerCmd)
}
