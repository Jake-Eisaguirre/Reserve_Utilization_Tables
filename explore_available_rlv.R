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
week_prior <- current_date - 50  # Set date 3 days prior to today
week_prior_pairing_date <- current_date -50  # Set date 7 days prior to today
fut_date_bid <- Sys.Date() + 7 # Set a future date (7 days from today)
fut_date <- Sys.Date() +10
#previous_bid_period <- substr(as.character((current_date - 30)), 1, 7)  # Get previous month and year
update_dt_rlv <- paste0((as.character(format(seq(fut_date_bid, by = "-1 month", length = 2)[2], "%Y-%m"))), "-25 00:00:00")  # Set relevant update date
#update_dt_rlv <- "2024-10-25 00:00:00"


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
  filter(CREW_INDICATOR == "FA",
         #BID_PERIOD == "2024-12"
         ) %>%  # Filter for flight attendants
  group_by(CREW_ID) %>% 
  filter(any(TRANSACTION_CODE %in% c("RSV", "RLV"))) %>%  # Filter for RSV and RLV transaction codes
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Create update_dt combining date and time
  #filter(update_dt < update_dt_rlv) %>% 
  select(CREW_ID, PAIRING_DATE) %>% 
  distinct() %>% 
  filter(!CREW_ID %in% c("1", "10"))


#18751 shows error - Problem is they are RLV -> ASN (16th-19th) ->FLP. Thus we are removing RLV even though
#they get another RLV -> ASN -> FLP on the 17th. So we lose 17th RLV.
# Filter data for flight attendants with transaction codes (RSV, RLV) and remove duplicates
fa_ut_rlv_int_a <- master_history_raw %>% 
  filter(!CREW_ID %in% c("1", "10")) %>% 
  filter(
    CREW_INDICATOR == "FA",
    CREW_ID %in% fa_ut_rlv_list$CREW_ID,
    PAIRING_DATE %in% fa_ut_rlv_list$PAIRING_DATE
  ) %>%
  group_by(CREW_ID, PAIRING_DATE) %>%
  filter(any(TRANSACTION_CODE %in% c("RSV", "RLV"))) %>% # Ensure at least one RLV in the group
  mutate(
    update_dt = as.POSIXct(paste(UPDATE_DATE, UPDATE_TIME, sep = " "), format = "%Y-%m-%d %H:%M:%S")
  ) %>%
  arrange(update_dt) %>%  # Arrange transactions by timestamp
  mutate(
    # Find the position of the last RLV code
    last_rlv_pos = max(which(TRANSACTION_CODE %in% c("RSV", "RLV"))),
    # Identify if SNO/GDO/OFF occurs after the last RLV
    occurs_after_last_rlv = row_number() > last_rlv_pos & TRANSACTION_CODE %in% c("SNO", "GDO", "OFF", "DRP"),
    # find the postion of the last SNO, GDO, OFF
    last_dropcode = max(which(TRANSACTION_CODE %in% c("RSV", "RLV", "ASN"))),
    # identify if FLP oaccurs after drop code
    occurs_last_sno = row_number() > last_dropcode & TRANSACTION_CODE %in% c("FLP", "FLU", "2SK", "FLV", "FLS",
                                                                              "SOP", "REM", "FAR", "MIL")
  )

  # Keep groups where the flag is FALSE
fa_ut_rlv <-  fa_ut_rlv_int %>% 
  filter(!any(occurs_after_last_rlv)) %>%
  ungroup() %>% 
  filter(TRANSACTION_CODE %in% c("RSV", "RLV")) %>%# Filter for updates before a specific date
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  # Assign unique ID to each group
  filter(!duplicated(temp_id)) %>%  # Remove duplicates based on temp_id
  ungroup() %>%
  select(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_DATE, PAIRING_POSITION, BID_PERIOD, BASE) %>%  # Select relevant columns
  mutate(EQUIPMENT = "NA")  # Set EQUIPMENT as "NA"
  
  
# fa_ut_rlv <- master_history_raw %>%
#   filter(CREW_INDICATOR == "FA",
#          CREW_ID %in% fa_ut_rlv_list$CREW_ID,
#          PAIRING_DATE %in% fa_ut_rlv_list$PAIRING_DATE,
#          #BID_PERIOD == "2024-12"
#   ) %>% 
#   group_by(CREW_ID, PAIRING_DATE) %>% 
#   filter(any(TRANSACTION_CODE %in% c("RLV"))) %>%  # Filter for RSV and RLV transaction codes
#   mutate(update_dt = as.POSIXct(paste(UPDATE_DATE, UPDATE_TIME, sep = " "), format = "%Y-%m-%d %H:%M:%S")) %>%  
#   arrange(update_dt) %>%  # Arrange by update timestamp
#   mutate(
#     # Identify if RLV occurs
#     has_rlv = cumsum(TRANSACTION_CODE %in% c("RLV")) > 0,
#     # Create a flag if SNO/GDO/OFF occurs after RLV
#     occurs_after_rlv = any(has_rlv & lag(TRANSACTION_CODE %in% c("SNO", "GDO", "OFF"), default = FALSE))
#   ) %>%
#   # Remove groups where the condition is met
#   filter(!occurs_after_rlv == TRUE)


# How many Reserves we Paid
fa_ut_asn <- master_history_raw %>% 
  filter(!CREW_ID %in% c("1", "10")) %>% 
  mutate(update_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  
  filter(CREW_INDICATOR == "FA") %>%
  group_by(CREW_ID, PAIRING_DATE) %>%
  arrange(update_dt) %>%  # Arrange transactions by timestamp
  mutate(
    # Find the position of the last ASN code, default to 0 if not found
    last_asn_pos = ifelse(any(TRANSACTION_CODE == "ASN"), max(which(TRANSACTION_CODE == "ASN")), 0),
    # Identify if SNO/GDO/OFF occurs after the last ASN
    occurs_after_last_asn = row_number() > last_asn_pos & TRANSACTION_CODE %in% c("SNO", "GDO", "OFF")
  ) %>%
  # Keep groups where the flag is FALSE
  filter(!any(occurs_after_last_asn)) %>%
  ungroup() %>% 
  filter(TRANSACTION_CODE %in% c("ASN")) %>%  
  group_by(CREW_ID, PAIRING_DATE, TRANSACTION_CODE) %>%
  mutate(temp_id = cur_group_id()) %>%  
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
  group_by(CREW_ID, PAIRING_NO, update_dt) %>%
  pad() %>%  # Fill missing dates (pad time series)
  ungroup() %>%
  mutate(EQUIPMENT = "NA") %>%
  rename(PAIRING_DATE = DATE) %>%
  select(!c(PAIRING_NO, update_dt, single, name, temp_id)) %>%
  fill(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_POSITION, BID_PERIOD, BASE, .direction = "down") %>% 
  distinct()# Fill missing values






# remove ANS's for 13th & 14th if person was RLV on 10-12th but got ASN for 12-14th.

remove_rlv <- fa_ut_rlv %>% 
  select(!TRANSACTION_CODE)

asn_remove_no_rlv <- rbind(fa_ut_single, fa_ut_double) %>% 
  group_by(PAIRING_DATE, CREW_ID) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  select(!temp_id) %>% 
  ungroup() %>% 
  select(!TRANSACTION_CODE) %>% 
  anti_join(remove_rlv) %>% 
  mutate(TRANSACTION_CODE = "ASN")

asn <- rbind(fa_ut_single, fa_ut_double) %>% 
  group_by(PAIRING_DATE, CREW_ID) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  select(!temp_id) %>% 
  ungroup() %>% 
  anti_join(asn_remove_no_rlv)


# remove RLV for multi day pairings: if RLV 12-14th and get ASN 12-14th then remove 13th-14th RLV. 

# get ride of any over lap if fls occurs but multi day ASN the day beofore and gets removed

flu_removes <- fa_ut_rlv_int %>% 
  filter(occurs_last_sno == TRUE) %>% 
  select(CREW_ID, PAIRING_DATE) %>% 
  distinct()
  
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
  rename(PAIRING_DATE = DATE) %>% 
  fill(CREW_INDICATOR, CREW_ID, TRANSACTION_CODE, PAIRING_POSITION, BID_PERIOD, BASE, .direction = "down") %>% 
  anti_join(flu_removes, by = c("CREW_ID", "PAIRING_DATE"))


fa_ut_rlv_filtered <- anti_join(fa_ut_rlv, remove_from_init_count, by = c("CREW_ID", "PAIRING_DATE", "BASE", "CREW_INDICATOR")) %>% 
  group_by(PAIRING_DATE, CREW_ID) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  select(!temp_id) %>% 
  ungroup()



asn_comb <- rbind(fa_ut_rlv_filtered, asn)

t <- asn_comb %>%
  filter(BASE == "HNL") %>% 
  mutate(TRANSACTION_CODE = if_else(TRANSACTION_CODE == "RSV", "RLV", TRANSACTION_CODE)) %>%
  group_by(PAIRING_DATE, TRANSACTION_CODE, BASE) %>%
  reframe(n = n()) %>% 
  pivot_wider(
    names_from = TRANSACTION_CODE, 
    values_from = n, 
    values_fill = 0
  ) %>%
  mutate(perc_utl = round(ASN / (RLV), 2) *100)




