package cmd

import (
	"fmt"

	"likenft-indexer/internal/database"

	"github.com/spf13/cobra"
)

var RefreshCountsCmd = &cobra.Command{
	Use:   "refresh-counts",
	Short: "Refresh the owner-count materialized views (one-shot)",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		dbService := database.New()
		defer dbService.Close()
		if err := dbService.RefreshOwnerCountViews(ctx); err != nil {
			panic(err)
		}
		fmt.Println("Owner count materialized views refreshed successfully")
	},
}
