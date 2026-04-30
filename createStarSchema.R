################################################################################
# File: createStarSchema.R
# Purpose: Create the dimensional star schema (dim_* + partitioned fact table)
#          on the analytical MySQL warehouse.
# Authors: Karthik Bharadwaj and Akshay Mambakkam Sridharan
################################################################################

rm(list = ls())

if (!require("DBI")) install.packages("DBI")
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("RSQLite")) install.packages("RSQLite")

library(RSQLite)
library(RMySQL)
library(DBI)

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

  mySqlDB <- tryCatch(
    {
      dbConnect(RMySQL::MySQL(),
                user = db_user,
                password = db_pwd,
                dbname = db_name,
                host = db_host,
                port = db_port
      )
    },
    error = function(e) {
      cat("Error connecting to MySQL:", e$message, "\n")
      stop("Could not connect to MySQL database")
    }
  )
  cat("Successfully connected to MySQL\n")
  
  return(mySqlDB)
}

disconnectMySql <- function(mySqlDB) {
  dbDisconnect(mySqlDB)
  cat("\nDisconnected from MySQL DB\n")
}

# Dimension table: Date attributes for time analysis
createDimDate <- function(conn) {
  query <- "CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10),
    INDEX idx_date (date_value),
    INDEX idx_year (year),
    INDEX idx_quarter (year, quarter),
    INDEX idx_month (year, month)
  )"
  
  dbExecute(conn, query)
  cat("Created dimension table: dim_date\n")
}

# Dimension table: Country information
createDimCountry <- function(conn) {
  query <- "CREATE TABLE IF NOT EXISTS dim_country (
    country_key INT AUTO_INCREMENT PRIMARY KEY,
    country_id INT NOT NULL,
    country VARCHAR(100) NOT NULL,
    UNIQUE KEY unique_country (country_id, country),
    INDEX idx_country_id (country_id),
    INDEX idx_country_name (country)
  )"
  
  dbExecute(conn, query)
  cat("Created dimension table: dim_country\n")
}

# Dimension table: Sport categories
createDimSport <- function(conn) {
  query <- "CREATE TABLE IF NOT EXISTS dim_sport (
    sport_key INT AUTO_INCREMENT PRIMARY KEY,
    sport VARCHAR(50) NOT NULL UNIQUE,
    INDEX idx_sport (sport)
  )"
  
  dbExecute(conn, query)
  cat("Created dimension table: dim_sport\n")
}

# Dimension table: Device types
createDimDevice <- function(conn) {
  query <- "CREATE TABLE IF NOT EXISTS dim_device (
    device_key INT AUTO_INCREMENT PRIMARY KEY,
    device_type VARCHAR(50) NOT NULL UNIQUE,
    INDEX idx_device (device_type)
  )"
  
  dbExecute(conn, query)
  cat("Created dimension table: dim_device\n")
}

# Dimension table: Streaming quality levels
createDimQuality <- function(conn) {
  query <- "CREATE TABLE IF NOT EXISTS dim_quality (
    quality_key INT AUTO_INCREMENT PRIMARY KEY,
    quality_streamed VARCHAR(20) NOT NULL UNIQUE,
    INDEX idx_quality (quality_streamed)
  )"
  
  dbExecute(conn, query)
  cat("Created dimension table: dim_quality\n")
}

# Fact table with RANGE partitioning by year for query optimization
# Foreign keys omitted due to MySQL limitation with partitioned tables
createFactStreamingTable <- function(conn) {
  query <- "CREATE TABLE IF NOT EXISTS fact_streaming_analytics (
    fact_id INT AUTO_INCREMENT,
    date_key INT NOT NULL,
    country_key INT NOT NULL,
    sport_key INT NOT NULL,
    device_key INT,
    quality_key INT,
    transaction_count INT DEFAULT 0,
    total_minutes_streamed DECIMAL(12, 2) DEFAULT 0,
    avg_minutes_per_transaction DECIMAL(10, 2) DEFAULT 0,
    completed_count INT DEFAULT 0,
    completion_rate DECIMAL(5, 2) DEFAULT 0,
    partition_year INT NOT NULL,
    PRIMARY KEY (fact_id, partition_year),
    INDEX idx_date_key (date_key),
    INDEX idx_country_key (country_key),
    INDEX idx_sport_key (sport_key),
    INDEX idx_device_key (device_key),
    INDEX idx_quality_key (quality_key),
    INDEX idx_date_country (date_key, country_key),
    INDEX idx_date_sport (date_key, sport_key),
    INDEX idx_composite (date_key, country_key, sport_key)
  ) PARTITION BY RANGE(partition_year) (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN MAXVALUE
  )"
  
  dbExecute(conn, query)
  cat("Created fact table: fact_streaming_analytics\n")
  cat("  - Partitioned by year using RANGE partitioning\n")
  cat("  - Indexed on dimension keys and common query patterns\n")
  cat("  - Foreign keys omitted (MySQL partitioning limitation)\n")
}

# Verify schema creation and display partition information
verifySchema <- function(conn) {
  cat("\n=== Schema Verification ===\n")
  
  tables <- dbListTables(conn)
  cat("Tables created:", paste(tables, collapse = ", "), "\n\n")
  
  if ("fact_streaming_analytics" %in% tables) {
    cat("Partition information:\n")
    partitions <- dbGetQuery(conn, "
      SELECT 
        PARTITION_NAME as PartitionName,
        PARTITION_METHOD as Method,
        PARTITION_EXPRESSION as Expression,
        PARTITION_DESCRIPTION as Description
      FROM INFORMATION_SCHEMA.PARTITIONS
      WHERE TABLE_NAME = 'fact_streaming_analytics'
      AND TABLE_SCHEMA = DATABASE()
      ORDER BY PARTITION_ORDINAL_POSITION
    ")
    print(partitions)
  }
}

# Summary statistics table for storing aggregate metrics
createSummaryTable <- function(conn) {
  query <- "CREATE TABLE IF NOT EXISTS summary_stats (
    stat_name VARCHAR(50) PRIMARY KEY,
    stat_value INT
  )"
  dbExecute(conn, query)
  cat("Created table: summary_stats\n")
}

main <- function() {
  cat("\n=== Creating Star Schema ===\n\n")
  
  sqlConn <- connectToMySql()
  
  cat("Dropping existing tables...\n")
  tables <- c("fact_streaming_analytics", "dim_date", "dim_country", 
              "dim_sport", "dim_device", "dim_quality", "summary_stats")
  for (table in tables) {
    tryCatch({
      dbExecute(sqlConn, sprintf("DROP TABLE IF EXISTS %s", table))
      cat(sprintf("  Dropped: %s\n", table))
    }, error = function(e) {
      cat(sprintf("  Note: %s may not exist\n", table))
    })
  }
  
  cat("\n")
  
  # Create dimension tables (Kimball approach)
  cat("Creating dimension tables...\n")
  createDimDate(sqlConn)
  createDimCountry(sqlConn)
  createDimSport(sqlConn)
  createDimDevice(sqlConn)
  createDimQuality(sqlConn)
  
  cat("\n")
  
  # Create fact table with partitioning
  cat("Creating fact table...\n")
  createFactStreamingTable(sqlConn)
  
  cat("\n")
  
  createSummaryTable(sqlConn)
  
  verifySchema(sqlConn)
  
  disconnectMySql(sqlConn)
}

main()