print("Script is starting...")  # Prints a message indicating the start of the script

# Check if 'librarian' package is installed, if not, install it and load it
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# Load necessary packages using librarian, ensuring they are installed if missing
librarian::shelf(tidyverse, here, DBI, odbc, padr)

# Define current and related dates for query
current_date <- Sys.Date()  # Set current date to today's date
week_prior <- current_date - 3  # Set date 3 days prior to today
week_prior_pairing_date <- current_date - 7  # Set date 7 days prior to today
fut_date <- Sys.Date() + 7  # Set a future date (7 days from today)
previous_bid_period <- substr(as.character((current_date - 30)), 1, 7)  # Get previous month and year
update_dt_rlv <- paste0((as.character(previous_bid_period)), "-25 00:00:00")  # Set relevant update date
raw_date <- Sys.Date()  # Save today's date as raw_date

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

# Filter data for flight attendants with transaction codes (RSV, RLV) and remove duplicates
fa_ut_rlv <- master_history_raw %>%
  ungroup() %>%
  filter(CREW_INDICATOR == "FA") %>%  # Filter for flight attendants
  filter(TRANSACTION_CODE %in% c("RSV", "RLV")) %>%  # Filter for RSV and RLV transaction codes
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
  filter(update_dt < update_dt_rlv) %>%  # Filter for updates before a specific date
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_DATE, PAIRING_POSITION, BID_PERIOD, BASE) %>%  # Select relevant columns
  mutate(EQUIPMENT = "NA")  # Set EQUIPMENT as "NA"

# Filter data for flight attendants with ASN transaction code and remove duplicates
fa_ut_asn <- master_history_raw %>%
  ungroup() %>%
  filter(CREW_INDICATOR == "FA") %>%  # Filter for flight attendants
  filter(TRANSACTION_CODE %in% c("ASN")) %>%  # Filter for ASN transaction code
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO, PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE, update_dt)

# Process ASN records for single pairing dates
fa_ut_single <- fa_ut_asn %>%
  group_by(CREW_ID, PAIRING_NO) %>%
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>%  # Mark single-day pairings
  filter(single == 1) %>%  # Filter single-day pairings
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"), values_to = "DATE") %>%  # Reshape data from wide to long format
  group_by(CREW_ID, TRANSACTION_CODE, DATE, PAIRING_NO) %>%
  mutate(temp_id = cur_group_id()) %>%
  filter(!duplicated(temp_id)) %>%
  ungroup() %>%
  mutate(EQUIPMENT = "NA") %>%  # Set EQUIPMENT as "NA"
  rename(PAIRING_DATE = DATE) %>%
  select(!c(PAIRING_NO, update_dt, single, name, temp_id))  # Drop unnecessary columns

# Process ASN records for non-single pairing dates (doubles)
fa_ut_double <- fa_ut_asn %>%
  group_by(CREW_ID, PAIRING_NO) %>%
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>%  # Mark single-day pairings
  filter(single == 0) %>%  # Filter non-single-day pairings
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"), values_to = "DATE") %>%
  group_by(CREW_ID, TRANSACTION_CODE, DATE, PAIRING_NO) %>%
  mutate(temp_id = cur_group_id()) %>%
  filter(!duplicated(temp_id)) %>%
  ungroup() %>%
  group_by(CREW_ID, BASE, PAIRING_NO) %>%
  pad() %>%  # Fill missing dates (pad time series)
  ungroup() %>%
  mutate(EQUIPMENT = "NA") %>%
  rename(PAIRING_DATE = DATE) %>%
  select(!c(PAIRING_NO, update_dt, single, name, temp_id)) %>%
  fill(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_POSITION, BID_PERIOD, BASE, .direction = "down")  # Fill missing values

# Combine flight attendant data (RLV, single, and double) into a single dataset
fa_ut <- rbind(fa_ut_rlv, fa_ut_single, fa_ut_double) %>%
  filter(PAIRING_DATE <= fut_date) %>%
  mutate(TRANSACTION_CODE = if_else(TRANSACTION_CODE == "RSV", "RLV", TRANSACTION_CODE)) %>%
  mutate(EQUIPMENT = "NA")  # Set EQUIPMENT as "NA"

#%>% 
  ## Gold Tier Below
  
  # group_by(PAIRING_DATE, BASE, TRANSACTION_CODE) %>% 
  # summarise(DAILY_COUNT = n()) %>%  # Use summarise() instead of mutate() to avoid repeated counts
  # ungroup() %>%
  # pivot_wider(names_from = TRANSACTION_CODE, values_from = DAILY_COUNT) %>%
  # rename(RLV_SCR = RLV) %>% 
  # mutate(ASN = if_else(is.na(ASN), 0, ASN)) %>% 
  # drop_na(RLV_SCR) %>% 
  # mutate(PERCENT_UTILIZATION = round((ASN / RLV_SCR) * 100, 2)) %>% 
  # select(PAIRING_DATE, BASE, ASN, RLV_SCR, PERCENT_UTILIZATION) %>% 
  # mutate(PAIRING_POSITION = "FA") %>% 
  # relocate(PAIRING_POSITION, .before = PAIRING_DATE) %>% 
  # relocate(EQUIPMENT, .before = ASN)


# Connect to the 'PLAYGROUND' database with tryCatch to handle errors
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
matching_cols <- dplyr::intersect(colnames(present_ut), colnames(fa_ut))

# Select matching columns from the present and new datasets
match_present_fo <- present_ut %>%
  select(matching_cols)

final_append_match_cols <- fa_ut %>%
  select(matching_cols)

# Perform an anti-join to find records that need to be appended to the database
final_append <- anti_join(final_append_match_cols, match_present_fo, by = join_by(PAIRING_POSITION, PAIRING_DATE, BASE, EQUIPMENT))

# Append the new records to the 'AA_RESERVE_UTILIZATION' table
dbAppendTable(db_connection_pg, "AA_RESERVE_UTILIZATION", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(5)  # Pause for 5 seconds
print("Script finished successfully!")  # Indicate that the script completed
Sys.sleep(10)  # Pause for 10 seconds