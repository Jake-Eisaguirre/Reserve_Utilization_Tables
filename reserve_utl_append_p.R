print("Script is starting...")  # Prints message indicating the script is starting

# Check if 'librarian' package is installed, if not, install it and load it
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# Load necessary packages using librarian, ensuring they are installed if missing
librarian::shelf(tidyverse, here, DBI, odbc, padr)

# Define dates for query
current_date <- Sys.Date()  # Set current date to today's date
week_prior <- current_date - 10  # Set date 3 days prior to today
week_prior_pairing_date <- current_date - 7  # Set date 7 days prior to today
previous_bid_period <- substr(as.character((current_date)), 1, 7)
fut_bid_period <- substr(as.character((current_date + 30)), 1, 7) # Get year and month as the previous bid period
fut_date <- Sys.Date() + 30  # Set a future date (7 days from today)
#update_dt_rlv <- paste0((as.character(format(seq(fut_date_bid, by = "-1 month", length = 2)[2], "%Y-%m"))), "-21 00:00:00")  # Set relevant update date

# Try to connect to the Snowflake database with tryCatch to handle errors
tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),
                                  Driver="SnowflakeDSIIDriver",
                                  Server="hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                  WAREHOUSE="DATA_LAKE_READER",
                                  Database="ENTERPRISE",
                                  UID= Sys.getenv("UID"),  # User ID pulled from environment variable
                                  authenticator = "externalbrowser")
  print("Database Connected!")  # Print success message if connected
},
error=function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set the schema search path to 'CREW_ANALYTICS'
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS")

# Define query to retrieve master history data based on pairing date range
q_master_history <- paste0("SELECT * FROM CT_MASTER_HISTORY WHERE PAIRING_DATE BETWEEN '", week_prior, "' AND '", fut_date, "';")

# Execute query and read in the master history data, converting time and date columns to character type
master_history_raw <- dbGetQuery(db_connection, q_master_history) %>%
  mutate(UPDATE_TIME = as.character(UPDATE_TIME),
         UPDATE_DATE = as.character(UPDATE_DATE))

# Filter data for pilots with specific transaction codes (ARC, SCR) and remove duplicates
pilot_ut_scr <- master_history_raw %>% 
  ungroup() %>% 
  filter(CREW_INDICATOR == "P") %>%  # Filter for pilots
  filter(TRANSACTION_CODE %in% c("ARC", "SCR")) %>%  # Filter for ARC and SCR transaction codes
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
  #filter(update_dt < update_dt_rlv) %>%  # Filter for updates before a specific date
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE)

# Filter data for pilots with ASN transaction code and remove duplicates
pilot_ut_asn <- master_history_raw %>% 
  ungroup() %>% 
  filter(CREW_INDICATOR == "P") %>%  # Filter for pilots
  filter(TRANSACTION_CODE %in% c("ASN")) %>%  # Filter for ASN transaction code
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO, PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE)

# Query to get master schedule data for the previous bid period
q_master_sched <- paste0("select CREW_ID, EFFECTIVE_FROM_DATE, EFFECTIVE_TO_DATE, EQUIPMENT, PAIRING_POSITION, BID_DATE,
                   UPDATE_DATE, UPDATE_TIME, BASE, BID_TYPE
                   from CT_MASTER_SCHEDULE WHERE BID_DATE between '", previous_bid_period, "' AND '",fut_bid_period,"';")

# Execute query and read in the master schedule data
raw_ms <- dbGetQuery(db_connection, q_master_sched)

# Clean the master schedule data: remove duplicates, keep latest records, and rename columns
clean_ms <- raw_ms %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
  group_by(CREW_ID, EFFECTIVE_FROM_DATE, EFFECTIVE_TO_DATE, PAIRING_POSITION) %>% 
  filter(update_dt == max(update_dt)) %>%  # Keep the latest record for each group
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  rename(BID_PERIOD = BID_DATE) %>%  # Rename BID_DATE to BID_PERIOD
  ungroup() %>%
  select(CREW_ID, BID_PERIOD, PAIRING_POSITION, EQUIPMENT, BASE)  # Select relevant columns

# Join and filter pilot SCR transaction data with cleaned master schedule data
emp_hist_p_scr <-  pilot_ut_scr %>% 
  inner_join(clean_ms, by = c("CREW_ID", "BID_PERIOD", "PAIRING_POSITION", "BASE")) %>%  # Inner join on multiple columns
  filter(PAIRING_POSITION %in% c("CA", "FO")) %>%  # Filter for Captain and First Officer
  select(!c(TO_DATE))  # Drop the TO_DATE column

# Join and filter pilot ASN transaction data with cleaned master schedule data
emp_hist_p_asn <-  pilot_ut_asn %>% 
  inner_join(clean_ms, by = c("CREW_ID", "BID_PERIOD", "PAIRING_POSITION", "BASE")) %>%  # Inner join on multiple columns
  filter(PAIRING_POSITION %in% c("CA", "FO"))  # Filter for Captain and First Officer

# Process ASN records with different pairing and to dates (doubles), pad missing dates
asn_double <- emp_hist_p_asn %>%
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>%  # Mark rows where PAIRING_DATE equals TO_DATE
  filter(single == 0) %>%  # Filter for non-single-day pairings
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"), values_to = "DATE") %>%  # Reshape data from wide to long format
  group_by(CREW_ID, EQUIPMENT, TRANSACTION_CODE, DATE) %>% 
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>% 
  group_by(CREW_ID, TRANSACTION_CODE, EQUIPMENT, PAIRING_NO) %>% 
  pad(by='DATE') %>%  # Fill in missing dates (pad time series)
  ungroup() %>% 
  rename(PAIRING_DATE = DATE) %>%  # Rename DATE to PAIRING_DATE
  select(!c(PAIRING_NO, single, name, temp_id))  %>%  # Drop unnecessary columns
  fill(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_POSITION, BID_PERIOD, BASE, .direction = "down")  # Fill missing values

# Process ASN records for single pairing dates
asn_single <- emp_hist_p_asn %>%
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>%  # Mark rows where PAIRING_DATE equals TO_DATE
  filter(single == 1) %>%  # Filter for single-day pairings
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"), values_to = "DATE") %>%  # Reshape data from wide to long format
  group_by(CREW_ID, EQUIPMENT, TRANSACTION_CODE, DATE) %>% 
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  rename(PAIRING_DATE = DATE) %>%  # Rename DATE to PAIRING_DATE
  select(!c(PAIRING_NO, single, name, temp_id))  # Drop unnecessary columns

# wrap_time <- master_history_raw %>% 
#   filter(TRANSACTION_CODE %in% c("ARC", "SCR"),
#          is.na(PAIRING_NO)) %>% 
#   mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " "),
#          wrap_dif = TO_TIME - FROM_TIME) %>% 
#   group_by(CREW_ID, PAIRING_DATE) %>% 
#   filter(wrap_dif == max(wrap_dif)) %>% 
#   ungroup() %>% 
#   group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>% 
#   mutate(temp_id = cur_group_id()) %>% 
#   filter(!duplicated(temp_id)) %>% 
#   ungroup() %>% 
#   select(CREW_ID, PAIRING_DATE, FROM_TIME, TO_TIME) 

# Combine SCR and ASN (single and double) pilot data into a single dataset
utl_p <- rbind(emp_hist_p_scr, asn_single, asn_double) %>% 
  ungroup() %>% 
  mutate(BASE = if_else(EQUIPMENT == "717", "HNL", BASE)) %>%  # Set BASE to HNL for 717 equipment
  filter(PAIRING_DATE <= fut_date) %>%  # Filter data up to the future date
  mutate(TRANSACTION_CODE = if_else(TRANSACTION_CODE == "ARC", "SCR", TRANSACTION_CODE)) %>%  # Replace "ACR" with "SCR"
  filter(!EQUIPMENT == "33Y")  # Exclude records with equipment "33Y"

  ## Gold Tier Below
  # 
  # group_by(BASE, PAIRING_DATE, PAIRING_POSITION, EQUIPMENT, TRANSACTION_CODE) %>% 
  # summarise(DAILY_COUNT = n()) %>% 
  # ungroup() %>%
  # pivot_wider(names_from = TRANSACTION_CODE, values_from = DAILY_COUNT) %>%  
  # mutate(ASN = if_else(is.na(ASN), 0, ASN)) %>% 
  # mutate(PERCENT_UTILIZATION = round((ASN / RLV_SCR) * 100, 2)) %>% 
  # select(PAIRING_DATE, PAIRING_POSITION, BASE, EQUIPMENT, ASN, RLV_SCR, PERCENT_UTILIZATION)%>% 
  # ungroup() %>% 
  # mutate(flag = if_else(PAIRING_DATE < 2024-04-31 & EQUIPMENT == 789, 1, 0)) %>% 
  # filter(flag == 0) %>% 
  # select(!flag) %>% 
  # drop_na(RLV_SCR)


# Connect to the 'PLAYGROUND' database
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",
                                     authenticator = "externalbrowser")
  print("Database Connected!")  # Print success message if connected
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set schema and query the 'AA_RESERVE_UTILIZATION' table
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")
present_ut <- dbGetQuery(db_connection_pg, "SELECT * FROM AA_RESERVE_UTILIZATION")

# Find matching columns between present data and the new dataset
matching_cols <- dplyr::intersect(colnames(present_ut), colnames(utl_p))

# Select matching columns from the present and new datasets
match_present_fo <- present_ut %>%
  select(matching_cols)

final_append_match_cols <- utl_p %>%
  select(matching_cols)

# Perform an anti-join to find records that need to be appended to the database
final_append <- anti_join(final_append_match_cols, match_present_fo)

# Append the new records to the 'AA_RESERVE_UTILIZATION' table
dbAppendTable(db_connection_pg, "AA_RESERVE_UTILIZATION", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(5)  # Pause for 5 seconds
print("Script finished successfully!")  # Indicate that the script completed
Sys.sleep(10)  # Pause for 10 seconds
