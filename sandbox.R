################################################################################
# Sandbox: MySQL Connection Test
# Purpose: Test connection to Aiven MySQL cloud database
################################################################################

# Clean environment
rm(list = ls())

# Package installation and loading
if (!require("DBI")) install.packages("DBI")
if (!require("RMySQL")) install.packages("RMySQL")

library(DBI)
library(RMySQL)

# Database credentials are read from environment variables (e.g. .Renviron).
db_host <- Sys.getenv("MYSQL_HOST")
db_port <- as.integer(Sys.getenv("MYSQL_PORT", unset = "3306"))
db_name <- Sys.getenv("MYSQL_DB")
db_user <- Sys.getenv("MYSQL_USER")
db_pwd  <- Sys.getenv("MYSQL_PWD")

if (any(c(db_host, db_name, db_user, db_pwd) == "")) {
  stop("Missing MySQL credentials. Set MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PWD in .Renviron.")
}

# Test connection
tryCatch(
  {
    cat("Attempting to connect to MySQL...\n")

    mydb <- dbConnect(RMySQL::MySQL(),
                      user = db_user,
                      password = db_pwd,
                      dbname = db_name,
                      host = db_host,
                      port = db_port
    )

    cat("Successfully connected to MySQL!\n\n")
    
    version <- dbGetQuery(mydb, "SELECT VERSION()")
    cat("MySQL Version:", version[[1]], "\n")
    
    tables <- dbListTables(mydb)
    cat("\nExisting tables in database:\n")
    if (length(tables) > 0) {
      print(tables)
    } else {
      cat("  (No tables found)\n")
    }

    cat("\nTesting simple query...\n")
    testQuery <- dbGetQuery(mydb, "SELECT 1+1 as result")
    cat("Query result:", testQuery$result, "\n")
    
    dbDisconnect(mydb)
    cat("\n✓ Connection closed successfully.\n")
  },
  error = function(e) {
    cat("\n✗ CONNECTION FAILED!\n")
    cat("Error message:", e$message, "\n")
    cat("\nTroubleshooting:\n")
    cat("  1. Check firewall settings (port 3306 must be open)\n")
    cat("  2. Verify database credentials\n")
    cat("  3. Ensure Aiven service is running\n")
    cat("  4. Check internet connection\n")
  }
)