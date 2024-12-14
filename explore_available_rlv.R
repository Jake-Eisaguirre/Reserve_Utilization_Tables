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
week_prior <- current_date - 60  # Set date 3 days prior to today
week_prior_pairing_date <- current_date -50  # Set date 7 days prior to today
fut_date_bid <- Sys.Date() + 7 # Set a future date (7 days from today)
fut_date <- Sys.Date() + 10
#previous_bid_period <- substr(as.character((current_date - 30)), 1, 7)  # Get previous month and year
update_dt_rlv <- paste0((as.character(format(seq(fut_date_bid, by = "-1 month", length = 2)[2], "%Y-%m"))), "-25 00:00:00")  # Set relevant update date



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

fa_ut_rlv_list <- master_history_raw %>%
  filter(CREW_INDICATOR == "FA") %>%  # Filter for flight attendants
  filter(any(TRANSACTION_CODE %in% c("RSV", "RLV"))) %>%  # Filter for RSV and RLV transaction codes
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
  filter(update_dt < update_dt_rlv) %>% 
  select(CREW_ID, PAIRING_DATE) %>% 
  distinct()



# Filter data for flight attendants with transaction codes (RSV, RLV) and remove duplicates
fa_ut_rlv <- master_history_raw %>%
  filter(CREW_INDICATOR == "FA",
         CREW_ID %in% fa_ut_rlv_list$CREW_ID,
         PAIRING_DATE %in% fa_ut_rlv_list$PAIRING_DATE) %>% 
  group_by(CREW_ID, PAIRING_DATE) %>% 
  filter(any(TRANSACTION_CODE %in% c("RLV", "RSV"))) %>%  # Filter for RSV and RLV transaction codes
  mutate(update_dt = as.POSIXct(paste(UPDATE_DATE, UPDATE_TIME, sep = " "), format = "%Y-%m-%d %H:%M:%S")) %>%  
  arrange(update_dt) %>%  # Arrange by update timestamp
  mutate(
    # Identify if RLV occurs
    has_rlv = cumsum(TRANSACTION_CODE == "RLV") > 0,
    # Create a flag if SNO/GDO/OFF occurs after RLV
    occurs_after_rlv = any(has_rlv & lag(TRANSACTION_CODE %in% c("SNO", "GDO", "OFF"), default = FALSE))
  ) %>%
  # Remove groups where the condition is met
  filter(!occurs_after_rlv == TRUE) %>%
  ungroup() %>% 
  filter(TRANSACTION_CODE %in% c("RLV", "RSV")) %>%# Filter for updates before a specific date
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_DATE, PAIRING_POSITION, BID_PERIOD, BASE) %>%  # Select relevant columns
  mutate(EQUIPMENT = "NA")  # Set EQUIPMENT as "NA"
  
  
#SNO, GDO, OFF

#Filter data for flight attendants with ASN transaction code and remove duplicates
# fa_ut_asn <- master_history_raw %>%
#   ungroup() %>%
#   group_by(CREW_ID, PAIRING_DATE) %>%
#   mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
#   filter(CREW_INDICATOR == "FA",
#          #update_dt == max(update_dt)
#   ) %>%  # Filter for flight attendants
#   filter(any(TRANSACTION_CODE %in% c("ASN"))) %>%  # Filter for ASN transaction code
#   group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE, update_dt) %>%
#   mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
#   filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
#   ungroup() %>%
#   select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO, PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE, update_dt)



#13779 2024-10 bid period logic

# How many Reserves we Paid
fa_ut_asn <- master_history_raw %>%
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  
  filter(CREW_INDICATOR == "FA") %>%
  group_by(CREW_ID, PAIRING_DATE) %>%
  arrange(update_dt) %>%
  mutate(last_asn_flag = cumsum(TRANSACTION_CODE == "ASN"),
         is_last_asn = TRANSACTION_CODE == "ASN" & last_asn_flag == max(last_asn_flag)) %>%
  mutate(after_last_asn = cumsum(is_last_asn) > 0) %>%
  mutate(sno_una_after_last_asn = any(after_last_asn & TRANSACTION_CODE %in% c("SNO", "UNA", "REM"))) %>%
  filter(!any(sno_una_after_last_asn)) %>%
  filter(is_last_asn == TRUE) %>%
  ungroup() %>%
  filter(TRANSACTION_CODE %in% c("ASN")) %>%  
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_NO, PAIRING_DATE, TO_DATE, PAIRING_POSITION, BID_PERIOD, BASE, update_dt)
#SNO, UNA,


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
  group_by(CREW_ID, PAIRING_NO, update_dt) %>%
  pad() %>%  # Fill missing dates (pad time series)
  ungroup() %>%
  mutate(EQUIPMENT = "NA") %>%
  rename(PAIRING_DATE = DATE) %>%
  select(!c(PAIRING_NO, update_dt, single, name, temp_id)) %>%
  fill(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_POSITION, BID_PERIOD, BASE, .direction = "down") %>% 
  distinct()# Fill missing values



## Old

remove_from_init_count <- fa_ut_asn %>%
  group_by(CREW_ID, PAIRING_NO) %>%
  mutate(single = if_else(PAIRING_DATE == TO_DATE, 1, 0)) %>%  # Mark single-day pairings
  filter(single == 0) %>%  # Filter non-single-day pairings
  pivot_longer(cols = c("PAIRING_DATE", "TO_DATE"), values_to = "DATE") %>%
  group_by(CREW_ID, TRANSACTION_CODE, DATE, PAIRING_NO) %>%
  mutate(temp_id = cur_group_id()) %>%
  filter(!duplicated(temp_id)) %>%
  ungroup() %>%
  group_by(CREW_ID, PAIRING_NO, update_dt) %>%
  pad() %>%  # Fill missing dates (pad time series)
  ungroup() %>% 
  mutate(flag = if_else(name == "TO_DATE" | is.na(name), "1", "0")) %>% 
  filter(flag == "1") %>% 
  rename(PAIRING_DATE = DATE)

#available reserves


rlv_asn_remove <- rbind(fa_ut_single, fa_ut_double) %>% 
  group_by(PAIRING_DATE, CREW_ID) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  select(!temp_id) %>% 
  ungroup()

fa_ut_rlv_filtered <- anti_join(fa_ut_rlv, remove_from_init_count, by = c("CREW_ID", "PAIRING_DATE")) %>% 
  group_by(PAIRING_DATE, CREW_ID) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  select(!temp_id) %>% 
  ungroup()



asn_comb <- rbind(fa_ut_rlv_filtered, rlv_asn_remove)

t <- asn_comb %>%
  mutate(TRANSACTION_CODE = if_else(TRANSACTION_CODE == "RSV", "RLV", TRANSACTION_CODE)) %>%
  group_by(PAIRING_DATE, TRANSACTION_CODE) %>%
  reframe(n = n()) %>% 
  pivot_wider(
    names_from = TRANSACTION_CODE, 
    values_from = n, 
    values_fill = 0
  ) %>%
  mutate(perc_utl = round(ASN / (RLV), 2) *100)




