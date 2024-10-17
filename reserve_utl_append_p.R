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
previous_bid_period <- substr(as.character((current_date)), 1, 7)

fut_date <-  Sys.Date() + 7


raw_date <- Sys.Date()


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


q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE PAIRING_DATE BETWEEN '", week_prior, "' AND '", fut_date, "';")

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


pilot_ut_asn <- master_history_raw %>% 
  ungroup() %>% 
  filter(CREW_INDICATOR == "P") %>% 
  filter(TRANSACTION_CODE %in% c("ASN")) %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  ungroup() %>% 
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO,
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
  filter(PAIRING_POSITION %in% c("CA", "FO")) %>% 
  select(!c(TO_DATE))


emp_hist_p_asn <-  pilot_ut_asn %>% 
  inner_join(clean_ms, by = c("CREW_ID", "BID_PERIOD", "PAIRING_POSITION", "BASE")) %>% 
  filter(PAIRING_POSITION %in% c("CA", "FO")) 


asn_double <- emp_hist_p_asn %>%
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>% 
  filter(single == 0) %>% 
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"),
               values_to = "DATE") %>% 
  group_by(CREW_ID, EQUIPMENT, TRANSACTION_CODE, DATE) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  ungroup() %>% 
  group_by(CREW_ID, TRANSACTION_CODE, EQUIPMENT, PAIRING_NO) %>% 
  pad(by='DATE') %>% 
  ungroup()%>% 
  rename(PAIRING_DATE = DATE) %>% 
  select(!c(PAIRING_NO, single, name, temp_id))  %>%
  fill(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_POSITION, BID_PERIOD, BASE, .direction = "down")

asn_single <- emp_hist_p_asn %>%
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>% 
  filter(single == 1) %>% 
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"),
               values_to = "DATE") %>% 
  group_by(CREW_ID, EQUIPMENT, TRANSACTION_CODE, DATE) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  ungroup() %>% 
  rename(PAIRING_DATE = DATE) %>% 
  select(!c(PAIRING_NO, single, name, temp_id))


utl_p <- rbind(emp_hist_p_scr, asn_single, asn_double)%>% 
  ungroup() %>% 
  mutate(BASE = if_else(EQUIPMENT == "717", "HNL", BASE)) %>% 
  filter(PAIRING_DATE <= fut_date) %>% 
  mutate(TRANSACTION_CODE = if_else(TRANSACTION_CODE == "ACR", "SCR", TRANSACTION_CODE)) %>% 
  filter(!EQUIPMENT == "33Y") %>% 
  
  ## Gold Tier Below
  
  group_by(BASE, PAIRING_DATE, PAIRING_POSITION, EQUIPMENT, TRANSACTION_CODE) %>% 
  summarise(DAILY_COUNT = n()) %>% 
  ungroup() %>%
  pivot_wider(names_from = TRANSACTION_CODE, values_from = DAILY_COUNT) %>%  
  mutate(ASN = if_else(is.na(ASN), 0, ASN)) %>% 
  mutate(PERCENT_UTILIZATION = round((ASN / RLV_SCR) * 100, 2)) %>% 
  select(PAIRING_DATE, PAIRING_POSITION, BASE, EQUIPMENT, ASN, RLV_SCR, PERCENT_UTILIZATION)%>% 
  ungroup() %>% 
  mutate(flag = if_else(PAIRING_DATE < 2024-04-31 & EQUIPMENT == 789, 1, 0)) %>% 
  filter(flag == 0) %>% 
  select(!flag) %>% 
  drop_na(RLV_SCR)


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
matching_cols <- dplyr::intersect(colnames(present_ut), colnames(utl_p))

# Filter both datasets to have matching columns and append new records
match_present_fo <- present_ut %>%
  select(matching_cols)

final_append_match_cols <- utl_p %>%
  select(matching_cols)


final_append <- anti_join(final_append_match_cols, match_present_fo, by = join_by(PAIRING_POSITION, PAIRING_DATE, EQUIPMENT))

# Append new records to the `AA_FINAL_PAIRING` table
dbAppendTable(db_connection_pg, "AA_RESERVE_UTILIZATION", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(5)  # Pause for 10 seconds
print("Script finished successfully!")
Sys.sleep(10)  # Pause for 10 seconds

