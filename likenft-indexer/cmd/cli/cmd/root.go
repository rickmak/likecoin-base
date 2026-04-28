package cmd

import (
	"context"
	"os"

	"log/slog"

	"likenft-indexer/cmd/cli/config"
	appcontext "likenft-indexer/cmd/cli/context"
	"likenft-indexer/internal/evm"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "cli",
	Short: "Migration Backend CLI",
	Long:  `CLI to perform asset migration`,
}

func Execute(
	cfg *config.EnvConfig,
	logger *slog.Logger,
	evmQueryClient evm.EVMQueryClient,
	evmClient *evm.EvmClient,
) {
	ctx := context.Background()
	ctx = appcontext.WithConfigContext(ctx, cfg)
	ctx = appcontext.WithLoggerContext(ctx, logger)
	ctx = appcontext.WithEvmQueryClient(ctx, evmQueryClient)
	ctx = appcontext.WithEvmClient(ctx, evmClient)
	err := rootCmd.ExecuteContext(ctx)
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(AcquireContractEvents)
	rootCmd.AddCommand(ProcessAllEVMEventCmd)
	rootCmd.AddCommand(ProcessEVMEventCmd)
	rootCmd.AddCommand(RecalculateEvmEventsCmd)
	rootCmd.AddCommand(RefreshCountsCmd)
}
