print("Script is starting...")


if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc, padr)


raw_date <- Sys.Date()

previous_bid_period <- substr(as.character((raw_date)), 1, 7)

update_dt_rlv <- paste0(substr(as.character((raw_date)), 1, 7), "-25 00:00:00")



tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),
                                  Driver="SnowflakeDSIIDriver",
                                  Server="hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                  WAREHOUSE="DATA_LAKE_READER",
                                  Database="ENTERPRISE",
                                  UID= Sys.getenv("UID"), 
                                  authenticator = "externalbrowser")
  print("Database Connected!")
},
error=function(cond) {
  print("Unable to connect to Database.")
})

# Set search_path
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS")


q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE BID_PERIOD = '", previous_bid_period, "';")

master_history_raw <- dbGetQuery(db_connection, q_master_history) %>%
  mutate(UPDATE_TIME = as.character(UPDATE_TIME),
         UPDATE_DATE = as.character(UPDATE_DATE))



pilot_ut_scr <- master_history_raw %>% 
  ungroup() %>% 
  filter(CREW_INDICATOR == "P") %>% 
  filter(TRANSACTION_CODE %in% c("ARC", "SCR")) %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  ungroup() %>% 
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE,
         PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE)



q_master_sched <- paste0("select CREW_ID, EFFECTIVE_FROM_DATE, EFFECTIVE_TO_DATE, EQUIPMENT, PAIRING_POSITION, BID_DATE,
                   UPDATE_DATE, UPDATE_TIME, BASE, BID_TYPE
                   from CT_MASTER_SCHEDULE WHERE BID_DATE = '", previous_bid_period, "';")


raw_ms <- dbGetQuery(db_connection, q_master_sched)

clean_ms <- raw_ms %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>% 
  group_by(CREW_ID, EFFECTIVE_FROM_DATE, EFFECTIVE_TO_DATE, PAIRING_POSITION) %>% 
  filter(update_dt == max(update_dt)) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  rename(BID_PERIOD = BID_DATE) %>% 
  ungroup() %>% 
  select(CREW_ID, BID_PERIOD, PAIRING_POSITION, EQUIPMENT, BASE)


emp_hist_p_scr <-  pilot_ut_scr %>% 
  inner_join(clean_ms, by = c("CREW_ID", "BID_PERIOD", "PAIRING_POSITION", "BASE")) %>% 
  filter(PAIRING_POSITION %in% c("CA", "FO"))


# Connect to the `PLAYGROUND` database and append data if necessary
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",
                                     authenticator = "externalbrowser")
  print("Database Connected!")
}, error = function(cond) {
  print("Unable to connect to Database.")
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")
present_ut <- dbGetQuery(db_connection_pg, "SELECT * FROM AA_RESERVE_UTILIZATION")


# Find matching columns between the present and final pairings
matching_cols <- dplyr::intersect(colnames(present_ut), colnames(emp_hist_p_scr))

# Filter both datasets to have matching columns and append new records
match_present_fo <- present_ut %>%
  select(matching_cols)

final_append_match_cols <- emp_hist_p_scr %>%
  select(matching_cols)


final_append <- anti_join(final_append_match_cols, match_present_fo)

# Append new records to the `AA_FINAL_PAIRING` table
dbAppendTable(db_connection_pg, "AA_RESERVE_UTILIZATION", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(10)  # Pause for 10 seconds
print("Script finished successfully!")

