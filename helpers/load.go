package helpers

import (
	"database/sql"
	"fmt"
	"log"
)

func createGlobalNDVsForEachTable(dbConn *sql.DB, metaquery string) {
	// 0. Pre-exists
	createGlobalNDVStatsQuery := `
		CREATE TABLE IF NOT EXISTS global_ndv_stats (
		    table_name text,
		    column_name text,
		    ndv_est hll
		);
	`
	_, err := dbConn.Exec(createGlobalNDVStatsQuery)
	if err != nil {
		log.Panic("Unable to create global_ndv_stats table:", err)
	}

	// 1️. Fetch all tables
	tablesResult, err := dbConn.Query(metaquery)
	if err != nil {
		log.Panic("Unable to fetch meta tables:", err)
	}
	defer tablesResult.Close()

	columnsQuery := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = $1
	`

	// 2. Iterate tables
	for tablesResult.Next() {
		var tableName string
		if err := tablesResult.Scan(&tableName); err != nil {
			log.Println("Failed to scan table name:", err)
			continue
		}

		// 3️. Fetch columns for table
		columnRows, err := dbConn.Query(columnsQuery, tableName)
		if err != nil {
			log.Println("Failed to fetch columns for table:", tableName, err)
			continue
		}

		for columnRows.Next() {
			var columnName string
			if err := columnRows.Scan(&columnName); err != nil {
				log.Println("Failed to scan column name:", err)
				continue
			}

			// 4. Get the Table details
			tableDetailsQuery := fmt.Sprintf("SELECT hll_add_agg(hll_hash_any(%s)) FROM %s;",
				columnName,
				tableName)

			tableDetailsResults, err := dbConn.Query(tableDetailsQuery)
			if err != nil {
				log.Printf("Failed to fetch %s details: %v", tableName, err)
				continue
			}

			var hllValue string
			if tableDetailsResults.Next() {
				tableDetailsResults.Scan(&hllValue)
			}

			// 4️. Build NDV query (HLL)
			ndvQuery := `
				INSERT INTO global_ndv_stats (table_name, column_name, ndv_est)
				VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;
			`

			// 5️2. Execute NDV computation
			if _, err := dbConn.Exec(ndvQuery, tableName, columnName, hllValue); err != nil {
				log.Printf(
					"Failed to compute NDV for %s.%s: %v\n",
					tableName, columnName, err,
				)
				continue
			}

			log.Printf("Computed NDV for %s.%s\n", tableName, columnName)
		}

		columnRows.Close()
	}

	log.Println("Global NDV generation completed successfully ✔️")
}

// Loads the NDVs from global_ndv_stats table and store in a map to compute the join order.
func LoadNDVs(dbConn *sql.DB, refresh bool, metaquery string) (map[string]map[string]float64, error) {
	if refresh {
		createGlobalNDVsForEachTable(dbConn, metaquery)
	}

	loadQuery := "SELECT table_name, column_name, hll_cardinality(ndv_est) FROM global_ndv_stats"

	globalResultsRows, err := dbConn.Query(loadQuery)
	if err != nil {
		return nil, err
	}
	defer globalResultsRows.Close()

	stats := make(map[string]map[string]float64)

	for globalResultsRows.Next() {
		var tableName, columnName string
		var ndv float64

		globalResultsRows.Scan(&tableName, &columnName, &ndv)

		if _, ok := stats[tableName]; !ok {
			stats[tableName] = make(map[string]float64)
		}
		stats[tableName][columnName] = ndv
	}

	return stats, nil
}
