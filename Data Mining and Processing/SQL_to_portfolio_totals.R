# clear out the environment
rm(list = ls())

# load backgroud packages and set options
options(stringsAsFactors = FALSE) # globally stop turning strings into factors

# libraries
library(tidyverse)
library(magrittr)
library(purrr) # nesting data
library(DBI) # to connect with postgres database
library(dbplyr) # postgres
library(readxl) # reading xlsx files
library(googledrive) # pulling journals from cloud

dw <- config::get("datawarehouse_finance")
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = dw$dbname,
                      #host = dw$host, # if running locally
                      host = dw$networkhost,
                      port    = dw$port,
                      user    = dw$user,
                      password   = dw$password
)

# create a list of the sheets
tab_names <- as.list(excel_sheets(path = dw$path))

# bring in all of the filled out journal data from each account from SQL
for (i in tab_names) {
  # assigns variable name as sheetname+journal after pulling from SQL database
  assign(paste0(i,'journal'), dbReadTable(con, SQL(paste0(i, '.journal')), daily))
}

# nest all of the journal dataframes together
df_list = mget(ls(pattern = "journal"))

# function to clean up journals
journalMaster <- function(journal.list){
  
  journal.list %<>% dplyr::select(-c(nav,return,account,format))
  return(journal.list)
}

# apply the dplyr commands to each journal
# journals must be specified with variable name, not string/character
journals = lapply(df_list, journalMaster)

# combine into one big dataframe (though with duplicates)
big = do.call(rbind.data.frame, journals)

big %>% group_by(ticker, date) %>%
  summarise(holdings = sum(holdings),
            invested = sum(invested),
            adjClose = mean(adjClose)) %>%
  mutate(nav = holdings * adjClose,
         return = (nav - invested)/invested,
         date = lubridate::date(date)) -> master

# grab current holdings/returns as of most recent date transacted in the equity
master %>%
  group_by(ticker) %>%
  slice(which.max(date)) -> last

# grab holdings as of last date in dataframe (ensuring at least some ownership)
last.date = max(master$date)
yesterday = max(master$date) - as.difftime(1, unit="days")
master %>%
  filter(date == last.date & holdings > 0) -> current


# summary stats for the portfolio
current %>% 
  ungroup() %>%
  summarise(nav = sum(nav),
            md_return = median(return),
            md_inv = median(invested))

# create list of holdings and their type
current %>%
  ungroup() %>%
  select(ticker) -> equities

# save equities list and current df to sql (without schema)
dbWriteTable(con, SQL('equities'), equities, overwrite=T)
dbWriteTable(con, SQL('current'), current, overwrite=T)

# open current and info tables, merge to create current holdings with industry info, upload to sheets
equity_info = dbReadTable(con, SQL('public.equity_info')) %>%
  select(symbol, sector, industry, country, quoteType) %>%
  distinct()

current = left_join(current, equity_info, by = c('ticker' = 'symbol'))

# upload to google drive
file_url = dw$file_url
drive_auth(email = dw$email)
file_upload = drive_get(as_id(file_url))
write.csv(current, "current.csv", row.names = F)
(up = drive_update(file_upload, media = 'current.csv'))

### taxable account ###

# grab current holdings/returns as of most recent date transacted in the equity
taxablejournal %>% group_by(ticker, date) %>%
  summarise(holdings = sum(holdings),
            invested = sum(invested),
            adjClose = mean(adjClose)) %>%
  mutate(nav = holdings * adjClose,
         return = (nav - invested)/invested,
         date = lubridate::date(date)) -> taxable

taxable %>%
  group_by(ticker) %>%
  slice(which.max(date)) -> last_tax

# grab holdings as of last date in dataframe (ensuring at least some ownership)
last.date = lubridate::date(max(taxable$date))
yesterday = lubridate::date(max(master$date) - as.difftime(1, unit="days"))
taxable %>%
  filter(date == last.date & holdings > 0) -> current_tax

# summary stats for the portfolio
current_tax %>% 
  ungroup() %>%
  summarise(nav = sum(nav),
            md_return = median(return),
            md_inv = median(invested))

tax_portfolio = left_join(current_tax, equity_info, by = c('ticker' = 'symbol'))

# upload taxable
tax_url = dw$tax_url
file_upload = drive_get(as_id(tax_url))
write.csv(tax_portfolio, "tax.csv", row.names = F)
(up = drive_update(file_upload, media = 'tax.csv'))

