print("Script is starting...")


if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc, padr)



current_date <- Sys.Date()  # Today's date
week_prior <- current_date - 3  # Date three days prior
week_prior_pairing_date <- current_date - 7  # Date seven days prior

previous_bid_period <- substr(as.character((current_date - 30)), 1, 7)
update_dt_rlv <- paste0((as.character(previous_bid_period)), "-25 00:00:00")


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


q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE PAIRING_DATE BETWEEN '", week_prior, "' AND '", current_date, "';")

master_history_raw <- dbGetQuery(db_connection, q_master_history) %>%
  mutate(UPDATE_TIME = as.character(UPDATE_TIME),
         UPDATE_DATE = as.character(UPDATE_DATE))


fa_ut_rlv <- master_history_raw %>% 
  ungroup() %>% 
  filter(CREW_INDICATOR == "FA") %>% 
  filter(TRANSACTION_CODE %in% c("RSV", "RLV")) %>%
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  filter(update_dt < update_dt_rlv) %>%
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%
  filter(!duplicated(temp_id)) %>%
  ungroup() %>% 
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE,
         PAIRING_DATE, PAIRING_POSITION, BID_PERIOD, BASE) %>% 
  mutate(EQUIPMENT = "NA")

fa_ut_asn <- master_history_raw %>% 
  ungroup() %>% 
  filter(CREW_INDICATOR == "FA") %>% 
  filter(TRANSACTION_CODE %in% c("ASN")) %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%
  filter(!duplicated(temp_id)) %>%
  ungroup() %>% 
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO,
         PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE, update_dt)


fa_ut_single <- fa_ut_asn %>% 
  group_by(CREW_ID, PAIRING_NO) %>% 
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>% 
  filter(single == 1) %>% 
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"),
               values_to = "DATE") %>% 
  group_by(CREW_ID, TRANSACTION_CODE, DATE, PAIRING_NO) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>%
  ungroup() %>% 
  mutate(EQUIPMENT = "NA") %>% 
  rename(PAIRING_DATE = DATE) %>% 
  select(!c(PAIRING_NO, update_dt, single, name, temp_id))


fa_ut_double <- fa_ut_asn %>% 
  group_by(CREW_ID, PAIRING_NO) %>% 
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>% 
  filter(single == 0) %>% 
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"),
               values_to = "DATE") %>% 
  group_by(CREW_ID, TRANSACTION_CODE, DATE, PAIRING_NO) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>%
  ungroup() %>% 
  group_by(CREW_ID, BASE, PAIRING_NO) %>% 
  pad() %>% 
  ungroup() %>% 
  mutate(EQUIPMENT = "NA")%>% 
  rename(PAIRING_DATE = DATE) %>% 
  select(!c(PAIRING_NO, update_dt, single, name, temp_id)) %>%
  fill(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_POSITION, BID_PERIOD, BASE, .direction = "down")

fa_ut <- rbind(fa_ut_rlv, fa_ut_single, fa_ut_double) %>% 
  filter(PAIRING_DATE <= raw_date) %>% 
  mutate(TRANSACTION_CODE = if_else(TRANSACTION_CODE == "RSV", "RLV", TRANSACTION_CODE)) %>% 
  group_by(PAIRING_DATE, BASE, TRANSACTION_CODE) %>% 
  summarise(DAILY_COUNT = n()) %>%  # Use summarise() instead of mutate() to avoid repeated counts
  ungroup() %>%
  pivot_wider(names_from = TRANSACTION_CODE, values_from = DAILY_COUNT) %>%
  rename(RLV_SCR = RLV) %>% 
  mutate(ASN = if_else(is.na(ASN), 0, ASN)) %>% 
  drop_na(RLV_SCR) %>% 
  mutate(PERCENT_UTILIZATION = round((ASN / RLV_SCR) * 100, 2)) %>% 
  select(PAIRING_DATE, BASE, ASN, RLV_SCR, PERCENT_UTILIZATION) %>% 
  mutate(PAIRING_POSITION = "FA") %>% 
  relocate(PAIRING_POSITION, .before = PAIRING_DATE) %>% 
  mutate(EQUIPMENT = "NA")%>% 
  relocate(EQUIPMENT, .before = ASN)





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
matching_cols <- dplyr::intersect(colnames(present_ut), colnames(fa_ut))

# Filter both datasets to have matching columns and append new records
match_present_fo <- present_ut %>%
  select(matching_cols)

final_append_match_cols <- fa_ut %>%
  select(matching_cols)


final_append <- anti_join(final_append_match_cols, match_present_fo)

# Append new records to the `AA_FINAL_PAIRING` table
dbAppendTable(db_connection_pg, "AA_RESERVE_UTILIZATION", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(5)  # Pause for 10 seconds
print("Script finished successfully!")
Sys.sleep(10)  # Pause for 10 seconds
