################################################################################
# File: loadAnalyticsDB.R
# Purpose: ETL pipeline. Extracts streaming transactions from a SQLite OLTP
#          source and a CSV export, conforms them, populates the dimension
#          tables, aggregates to the fact grain, and bulk-loads the
#          partitioned fact table on the MySQL warehouse.
# Authors: Karthik Bharadwaj and Akshay Mambakkam Sridharan
################################################################################
rm(list = ls())

if (!require("DBI")) install.packages("DBI")
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("RSQLite")) install.packages("RSQLite")

library(RSQLite)
library(RMySQL)
library(DBI)

# Connects to the remote mysql database
connectToMySql <- function() {
  cat("Connecting to MySQL database...\n")

  db_host <- Sys.getenv("MYSQL_HOST")
  db_port <- as.integer(Sys.getenv("MYSQL_PORT", unset = "3306"))
  db_name <- Sys.getenv("MYSQL_DB")
  db_user <- Sys.getenv("MYSQL_USER")
  db_pwd  <- Sys.getenv("MYSQL_PWD")

  if (any(c(db_host, db_name, db_user, db_pwd) == "")) {
    stop("Missing MySQL credentials. Set MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PWD (e.g. in .Renviron).")
  }

  mySqlDB <- tryCatch({
    dbConnect(RMySQL::MySQL(),
              user = db_user,
              password = db_pwd,
              dbname = db_name,
              host = db_host,
              port = db_port)
  }, error = function(e) {
    cat("Error connecting to MySQL:", e$message, "\n")
    stop("Could not connect to MySQL database")
  })
  cat("Successfully connected to MySQL\n")
  return(mySqlDB)
}

# Terminates the connection to the remote mysql database
disconnectMySql <- function(mySqlDB) {
  dbDisconnect(mySqlDB)
  cat("\nDisconnected from MySQL DB\n")
}

# Connects to the sqlite database  
connectToSqlite <- function(sqliteDbString) {
  cat("Connecting to SQLite database...\n")
  
  if (!file.exists(sqliteDbString)) {
    cat("ERROR: Database file not found at:", sqliteDbString, "\n")
    stop("Could not find SQLite database file")
  }
  
  sqliteDb <- tryCatch({
    dbConnect(SQLite(), sqliteDbString)
  }, error = function(e) {
    cat("Error connecting to SQLite:", e$message, "\n")
    stop("Could not connect to SQLite database")
  })
  cat("Successfully connected to SQLite\n")
  return(sqliteDb)
}

# Disconnects from the sqlite database  
disconnectSqlite <- function(sqliteDb) {
  dbDisconnect(sqliteDb)
  cat("\nDisconnected from SQLite DB\n")
}

# Populate date dimension with all unique dates from both sources
populateDimDate <- function(mysqlConn, sqliteConn, csvPath) {
  cat("\n=== Populating dim_date ===\n")
  
  sqliteDates <- dbGetQuery(sqliteConn, "
    SELECT DISTINCT streaming_date FROM streaming_txns
  ")$streaming_date
  
  csvData <- read.csv(csvPath, stringsAsFactors = FALSE)
  csvDates <- unique(csvData$streaming_date)
  
  allDates <- unique(c(sqliteDates, csvDates))
  allDates <- as.Date(allDates)
  allDates <- allDates[!is.na(allDates)]
  
  cat("Found", length(allDates), "unique dates\n")
  
  # Create date dimension with date attributes
  dateDF <- data.frame(
    date_value = allDates,
    year = as.integer(format(allDates, "%Y")),
    quarter = as.integer((as.integer(format(allDates, "%m")) - 1) %/% 3 + 1),
    month = as.integer(format(allDates, "%m")),
    week = as.integer(format(allDates, "%V")),
    day_of_week = as.integer(format(allDates, "%w")),
    day_name = weekdays(allDates, abbreviate = TRUE)
  )
  
  # Bulk insert in batches
  batchSize <- 200
  for (i in seq(1, nrow(dateDF), by = batchSize)) {
    end <- min(i + batchSize - 1, nrow(dateDF))
    batch <- dateDF[i:end, ]
    
    values <- apply(batch, 1, function(row) {
      sprintf("('%s', %d, %d, %d, %d, %d, '%s')",
              row["date_value"],
              as.integer(row["year"]),
              as.integer(row["quarter"]),
              as.integer(row["month"]),
              as.integer(row["week"]),
              as.integer(row["day_of_week"]),
              row["day_name"])
    })
    
    insertQuery <- sprintf("
      INSERT IGNORE INTO dim_date 
      (date_value, year, quarter, month, week, day_of_week, day_name)
      VALUES %s
    ", paste(values, collapse = ","))
    
    dbExecute(mysqlConn, insertQuery)
  }
  
  count <- dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_date")$cnt
  cat("Inserted", count, "dates into dim_date\n")
}

# Populates the Country table
populateDimCountry <- function(mysqlConn, sqliteConn) {
  cat("\n=== Populating dim_country ===\n")
  
  countries <- dbGetQuery(sqliteConn, "SELECT country_id, country FROM countries")
  
  values <- apply(countries, 1, function(row) {
    sprintf("(%d, '%s')",
            as.integer(row["country_id"]),
            gsub("'", "''", row["country"]))
  })
  
  insertQuery <- sprintf("
    INSERT IGNORE INTO dim_country (country_id, country)
    VALUES %s
  ", paste(values, collapse = ","))
  
  dbExecute(mysqlConn, insertQuery)
  
  count <- dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_country")$cnt
  cat("Inserted", count, "countries into dim_country\n")
}

# Populates the Sport table
populateDimSport <- function(mysqlConn, sqliteConn) {
  cat("\n=== Populating dim_sport ===\n")
  
  sports <- dbGetQuery(sqliteConn, "SELECT DISTINCT sport FROM assets")
  
  values <- apply(sports, 1, function(row) {
    sprintf("('%s')", gsub("'", "''", row["sport"]))
  })
  
  insertQuery <- sprintf("
    INSERT IGNORE INTO dim_sport (sport)
    VALUES %s
  ", paste(values, collapse = ","))
  
  dbExecute(mysqlConn, insertQuery)
  
  count <- dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_sport")$cnt
  cat("Inserted", count, "sports into dim_sport\n")
}

# Populates the Device table
populateDimDevice <- function(mysqlConn, sqliteConn, csvPath) {
  cat("\n=== Populating dim_device ===\n")
  
  sqliteDevices <- dbGetQuery(sqliteConn, "
    SELECT DISTINCT device_type FROM streaming_txns WHERE device_type IS NOT NULL
  ")$device_type
  
  csvData <- read.csv(csvPath, stringsAsFactors = FALSE)
  csvDevices <- unique(csvData$device_type)
  
  allDevices <- unique(c(sqliteDevices, csvDevices))
  allDevices <- allDevices[!is.na(allDevices) & allDevices != ""]
  
  if (length(allDevices) > 0) {
    values <- sapply(allDevices, function(d) {
      sprintf("('%s')", gsub("'", "''", d))
    })
    
    insertQuery <- sprintf("
      INSERT IGNORE INTO dim_device (device_type)
      VALUES %s
    ", paste(values, collapse = ","))
    
    dbExecute(mysqlConn, insertQuery)
  }
  
  count <- dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_device")$cnt
  cat("Inserted", count, "device types into dim_device\n")
}

# Populates the Streaming Transactions table 
populateDimQuality <- function(mysqlConn, sqliteConn, csvPath) {
  cat("\n=== Populating dim_quality ===\n")
  
  sqliteQuality <- dbGetQuery(sqliteConn, "
    SELECT DISTINCT quality_streamed FROM streaming_txns WHERE quality_streamed IS NOT NULL
  ")$quality_streamed
  
  csvData <- read.csv(csvPath, stringsAsFactors = FALSE)
  csvQuality <- unique(csvData$quality_streamed)
  
  allQuality <- unique(c(sqliteQuality, csvQuality))
  allQuality <- allQuality[!is.na(allQuality) & allQuality != ""]
  
  if (length(allQuality) > 0) {
    values <- sapply(allQuality, function(q) {
      sprintf("('%s')", gsub("'", "''", q))
    })
    
    insertQuery <- sprintf("
      INSERT IGNORE INTO dim_quality (quality_streamed)
      VALUES %s
    ", paste(values, collapse = ","))
    
    dbExecute(mysqlConn, insertQuery)
  }
  
  count <- dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_quality")$cnt
  cat("Inserted", count, "quality levels into dim_quality\n")
}

# Extract data using SQL JOINs at database level for scalability 
extractFromSQLiteScalable <- function(sqliteConn) {
  cat("\n=== Extracting from SQLite ===\n")
  
  query <- "
    SELECT 
      st.streaming_date,
      st.minutes_streamed,
      st.completed,
      st.device_type,
      st.quality_streamed,
      a.sport,
      a.country_id,
      c.country
    FROM streaming_txns st
    INNER JOIN assets a ON st.asset_id = a.asset_id
    INNER JOIN countries c ON a.country_id = c.country_id
  "
  
  data <- dbGetQuery(sqliteConn, query)
  data$source <- "SQLite"
  
  cat("Extracted", nrow(data), "transactions from SQLite\n")
  return(data)
}

extractFromCSVScalable <- function(csvPath, sqliteConn) {
  cat("\n=== Extracting from CSV ===\n")
  
  csvData <- read.csv(csvPath, stringsAsFactors = FALSE)
  cat("Loaded", nrow(csvData), "rows from CSV\n")

  tempTable <- "temp_csv_data"
  dbWriteTable(sqliteConn, tempTable, csvData, overwrite = TRUE)
  
  query <- sprintf("
    SELECT 
      t.streaming_date,
      t.minutes_streamed,
      t.completed,
      t.device_type,
      t.quality_streamed,
      a.sport,
      a.country_id,
      c.country
    FROM %s t
    INNER JOIN assets a ON t.asset_id = a.asset_id
    INNER JOIN countries c ON a.country_id = c.country_id
  ", tempTable)
  
  data <- dbGetQuery(sqliteConn, query)
  data$source <- "CSV"
  
  dbExecute(sqliteConn, sprintf("DROP TABLE IF EXISTS %s", tempTable))
  
  cat("Extracted", nrow(data), "enriched transactions from CSV\n")
  return(data)
}

# Aggregate data and retrieve dimension keys for fact table
aggregateForFactTable <- function(data, mysqlConn) {
  cat("\n=== Aggregating Data ===\n")
  
  data$streaming_date <- as.Date(data$streaming_date)
  data$completed <- as.integer(data$completed)
  data$year <- as.integer(format(data$streaming_date, "%Y"))
  
  sqliteCount <- sum(data$source == "SQLite")
  csvCount <- sum(data$source == "CSV")
  
  cat("Pre-aggregation counts:\n")
  cat("  SQLite:", format(sqliteCount, big.mark = ","), "\n")
  cat("  CSV:", format(csvCount, big.mark = ","), "\n")
  
  # Aggregate using SQL functions
  aggregated <- aggregate(
    cbind(
      transaction_count = rep(1, nrow(data)),
      total_minutes_streamed = data$minutes_streamed,
      completed_count = data$completed
    ) ~ streaming_date + sport + country + country_id + 
      device_type + quality_streamed + year,
    data = data,
    FUN = sum
  )
  
  aggregated$avg_minutes_per_transaction <- 
    aggregated$total_minutes_streamed / aggregated$transaction_count
  aggregated$completion_rate <- 
    (aggregated$completed_count / aggregated$transaction_count) * 100
  
  # Retrieve surrogate keys from dimension tables
  dateKeys <- dbGetQuery(mysqlConn, "SELECT date_id, date_value FROM dim_date")
  aggregated <- merge(aggregated, dateKeys, 
                      by.x = "streaming_date", by.y = "date_value", all.x = TRUE)
  
  countryKeys <- dbGetQuery(mysqlConn, "SELECT country_key, country_id FROM dim_country")
  aggregated <- merge(aggregated, countryKeys, by = "country_id", all.x = TRUE)
  
  sportKeys <- dbGetQuery(mysqlConn, "SELECT sport_key, sport FROM dim_sport")
  aggregated <- merge(aggregated, sportKeys, by = "sport", all.x = TRUE)
  
  deviceKeys <- dbGetQuery(mysqlConn, "SELECT device_key, device_type FROM dim_device")
  aggregated <- merge(aggregated, deviceKeys, by = "device_type", all.x = TRUE)
  
  qualityKeys <- dbGetQuery(mysqlConn, "SELECT quality_key, quality_streamed FROM dim_quality")
  aggregated <- merge(aggregated, qualityKeys, by = "quality_streamed", all.x = TRUE)
  
  aggregated <- aggregated[complete.cases(aggregated[, c("date_id", "country_key", "sport_key")]), ]
  
  cat("Aggregated into", nrow(aggregated), "fact rows\n")
  
  return(list(
    factData = aggregated,
    sqliteTransactions = sqliteCount,
    csvTransactions = csvCount
  ))
}

# Bulk insert into fact table with batch processing
bulkInsertToFactTable <- function(mysqlConn, factData) {
  cat("\n=== Loading into Fact Table ===\n")
  
  batchSize <- 15000
  totalRows <- nrow(factData)
  numBatches <- ceiling(totalRows / batchSize)
  successCount <- 0
  
  for (i in 1:numBatches) {
    startIdx <- (i - 1) * batchSize + 1
    endIdx <- min(i * batchSize, totalRows)
    batch <- factData[startIdx:endIdx, ]
    
    values <- apply(batch, 1, function(row) {
      device_key_val <- if(!is.na(row["device_key"])) as.integer(row["device_key"]) else "NULL"
      quality_key_val <- if(!is.na(row["quality_key"])) as.integer(row["quality_key"]) else "NULL"
      
      sprintf("(%d, %d, %d, %s, %s, %d, %.2f, %.2f, %d, %.2f, %d)",
              as.integer(row["date_id"]),
              as.integer(row["country_key"]),
              as.integer(row["sport_key"]),
              device_key_val,
              quality_key_val,
              as.integer(row["transaction_count"]),
              as.numeric(row["total_minutes_streamed"]),
              as.numeric(row["avg_minutes_per_transaction"]),
              as.integer(row["completed_count"]),
              as.numeric(row["completion_rate"]),
              as.integer(row["year"]))
    })
    
    insertQuery <- sprintf("
      INSERT INTO fact_streaming_analytics 
      (date_key, country_key, sport_key, device_key, quality_key,
       transaction_count, total_minutes_streamed, avg_minutes_per_transaction,
       completed_count, completion_rate, partition_year)
      VALUES %s
    ", paste(values, collapse = ","))
    
    tryCatch({
      dbExecute(mysqlConn, insertQuery)
      successCount <- successCount + nrow(batch)
      cat(sprintf("Batch %d/%d: Inserted %d rows\n", i, numBatches, nrow(batch)))
    }, error = function(e) {
      cat(sprintf("Error in batch %d: %s\n", i, e$message))
    })
  }
  
  cat("Successfully inserted", successCount, "fact rows\n")
  return(successCount)
}

# Validate data integrity and completeness
validateFactTable <- function(mysqlConn, sqliteTransactions, csvTransactions) {
  cat("\n==========================================\n")
  cat("       FACT TABLE VALIDATION\n")
  cat("==========================================\n")
  
  totalFacts <- dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM fact_streaming_analytics")$cnt
  cat("\n[Fact Table]\n")
  cat("  Total fact rows:", format(totalFacts, big.mark = ","), "\n")
  
  totalTxns <- dbGetQuery(mysqlConn, "
    SELECT SUM(transaction_count) as total FROM fact_streaming_analytics
  ")$total
  cat("  Total transactions:", format(totalTxns, big.mark = ","), "\n")
  
  cat("\n[Source Validation]\n")
  cat("  Expected from SQLite:", format(sqliteTransactions, big.mark = ","), "\n")
  cat("  Expected from CSV:", format(csvTransactions, big.mark = ","), "\n")
  cat("  Total expected:", format(sqliteTransactions + csvTransactions, big.mark = ","), "\n")
  cat("  Actual in warehouse:", format(totalTxns, big.mark = ","), "\n")
  
  matchPct <- (totalTxns / (sqliteTransactions + csvTransactions)) * 100
  cat(sprintf("  Match rate: %.2f%%\n", matchPct))
  
  cat("\n[Partition Distribution]\n")
  partitionCounts <- dbGetQuery(mysqlConn, "
    SELECT partition_year, COUNT(*) as fact_rows, SUM(transaction_count) as transactions
    FROM fact_streaming_analytics
    GROUP BY partition_year
    ORDER BY partition_year
  ")
  print(partitionCounts)
  
  cat("\n[Dimension Counts]\n")
  dimCounts <- list(
    dates = dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_date")$cnt,
    countries = dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_country")$cnt,
    sports = dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_sport")$cnt,
    devices = dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_device")$cnt,
    quality = dbGetQuery(mysqlConn, "SELECT COUNT(*) as cnt FROM dim_quality")$cnt
  )
  cat("  Unique dates:", dimCounts$dates, "\n")
  cat("  Unique countries:", dimCounts$countries, "\n")
  cat("  Unique sports:", dimCounts$sports, "\n")
  cat("  Unique devices:", dimCounts$devices, "\n")
  cat("  Unique quality levels:", dimCounts$quality, "\n")
  
  cat("\n==========================================\n")
  if (matchPct > 99) {
    cat("VALIDATION PASSED\n")
  } else {
    cat("data missing\n")
  }
  cat("==========================================\n\n")
}

main <- function() {
  cat("\n===============================================\n")
  cat("    ETL PROCESS - Star Schema Loading\n")
  cat("===============================================\n")
  
  csvPath <- "data/new-streaming-transactions-98732.csv"
  sqliteConn <- connectToSqlite("data/subscribersDB.sqlitedb")
  mysqlConn <- connectToMySql()
  
  populateDimDate(mysqlConn, sqliteConn, csvPath)
  populateDimCountry(mysqlConn, sqliteConn)
  populateDimSport(mysqlConn, sqliteConn)
  populateDimDevice(mysqlConn, sqliteConn, csvPath)
  populateDimQuality(mysqlConn, sqliteConn, csvPath)
  
  sqliteData <- extractFromSQLiteScalable(sqliteConn)
  csvData <- extractFromCSVScalable(csvPath, sqliteConn)
  
  combinedData <- rbind(sqliteData, csvData)
  cat("\nCombined", nrow(combinedData), "total transactions\n")
  
  aggResult <- aggregateForFactTable(combinedData, mysqlConn)
  
  bulkInsertToFactTable(mysqlConn, aggResult$factData)
  
  validateFactTable(mysqlConn, aggResult$sqliteTransactions, aggResult$csvTransactions)
  
  uniqueUsers <- length(unique(dbGetQuery(sqliteConn, "SELECT DISTINCT user_id FROM streaming_txns")$user_id))
  dbExecute(mysqlConn, sprintf("REPLACE INTO summary_stats VALUES ('unique_users', %d)", uniqueUsers))
  
  disconnectSqlite(sqliteConn)
  disconnectMySql(mysqlConn)
  
  cat("\n===============================================\n")
  cat("    ETL PROCESS COMPLETE\n")
  cat("===============================================\n")
}

main()