# program to download journal ledger entries of purchases and sales
# which then calculates daily and total values/holdings for each account
# saving these to sql database

# load background packages and set options
options(stringsAsFactors = FALSE) # globally stop turning strings into factors

# libraries
library(tidyverse)
library(magrittr)
library(timetk) # for tk_tbl
library(googledrive) # pulling cloud data
library(purrr) # nesting data
library(readxl) # reading xlsx files
library(anytime) # pulling data by sysdate
library(googledrive) # pulling journals from cloud
library(DBI) # to connect with postgres database
library(dbplyr) # postgres

dw <- config::get("datawarehouse_finance")
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = dw$dbname,
                      #host = dw$host, # if running locally
                      host = dw$networkhost,
                      port    = dw$port,
                      user    = dw$user,
                      password   = dw$password
)

# set the dates of interest for data collection
first.date = '2016-06-30'
last.date <- anydate(Sys.Date())

# list the sheets
tab_names <- excel_sheets(path = dw$path)

# get the main ledger
folder_url = dw$folder_url
drive_auth(email = dw$email)
folder = drive_get(as_id(folder_url))
csv_files = drive_ls(folder, type='spreadsheet')
walk(csv_files$id, ~ drive_download(as_id(.x), overwrite = T))

# run the entire function for each journal/account
for (sheet in tab_names) {
  
  #### Get Journal Data ####
  getJournals <- function(spreadsheet){
    
    holdings = read_excel(path = dw$path, sheet = spreadsheet) %>%
      select(-c(sector, country, global, type)) %>%
      drop_na()
    
    return(holdings)
    
  }
  holdings = getJournals(sheet)
  
  # create list of assets need data for
  tickers = as.character(holdings$instrument)
  tickers = unique(tickers) # limits the list to only unique values
  tickers = na.omit(tickers)
  
  # create df of categorical assets (to help with merging later)
  categorical = holdings %>%
    select(instrument) %>%
    distinct()
  
  #### Get Historical Data ####
  getRiingoData <- function(first.date, tickers){
    # set date parameters
    first.date <- first.date
    last.date <- anydate(Sys.Date())
    
    # make df of all dates from beginning of time to today
    date_range = data.frame(date=seq(as.Date(first.date), as.Date(last.date), by="days"))
    
    library(riingo)
    library(quantmod)
    token = dw$token
    riingo_set_token(token, inform = TRUE)
    
    # get data from riingo
    ohlc_data = riingo_prices(tickers,
                              start_date = first.date,
                              end_date = last.date)
    
    return(ohlc_data)
  }
  ohlc_data = getRiingoData(first.date, tickers)
  
  #### Fill in Journal Dates with Daily Holdings ####
  fillJournal <- function(holdings, first.date, last.date){
    # takes transaction journal data and returns daily holdings
    
    nested = holdings %>% group_by(instrument) %>% nest() # nest journal data by ticker
    
    transactions <- data.frame()
    for (i in 1:nrow(nested)) {
      for (j in 1:nrow(nested))
        journaldata = as.data.frame(nested[i,]) # makes first row a shoddy df
      journaldata = do.call(cbind.data.frame, journaldata) # turns the list into a true df
      journaldata$date = lubridate::ymd(journaldata$data.timestamp) # makes timestamp a date
      b = data.frame(date=seq(as.Date(first.date), as.Date(last.date), by="days"))
      journaldata = left_join(b, journaldata, by='date')
      journaldata = na.locf(journaldata)
      journaldata[is.na(journaldata)] <- 0 # turn NAs to zeros
      transactions = rbind(transactions, journaldata)
    }
    
    transactions %<>% mutate(
      ticker = instrument,
      `data.holdings` = as.numeric(`data.holdings`),
      `data.invested` = as.numeric(`data.invested`)
    )
    
    return(transactions)
  }
  transactions = fillJournal(holdings, first.date, last.date)
  
  # merge the two datasets
  completedata <- left_join(ohlc_data, transactions, by=c("ticker","date")) %>%
    mutate(NAV = `data.holdings` * adjClose, # gives daily value of holdings
           Return = (NAV - `data.invested`) / `data.invested`) # daily return of holdings
  
  # create separate dataframes of daily returns
  daily_returns = completedata %>%
    select(date, ticker, Return) %>%
    pivot_wider(names_from = ticker, values_from = Return)
  
  daily_holdings = completedata %>%
    select(date, ticker, `data.holdings`) %>%
    pivot_wider(names_from = ticker, values_from = `data.holdings`)
  
  daily_NAV = completedata %>%
    select(date, ticker, NAV) %>%
    pivot_wider(names_from = ticker, values_from = NAV) %>%
    mutate(portfolioNAV = select(., 2:length(.)) %>% rowSums(na.rm=TRUE)) # skip date, but add across all others
  
  daily_invested = completedata %>%
    select(date, ticker, `data.invested`) %>%
    pivot_wider(names_from = ticker, values_from = `data.invested`) %>%
    mutate(portfolioInvested = select(., 2:length(.)) %>% rowSums(na.rm=TRUE)) # skip date, but add across all others
  
  daily_portfolio = data.frame(daily_NAV$date,
                               daily_NAV$portfolioNAV, 
                               daily_invested$portfolioInvested) %>%
    rename(NAV = `daily_NAV.portfolioNAV`,
           Invested = `daily_invested.portfolioInvested`,
           date = `daily_NAV.date`) %>% 
    mutate(Return = (NAV - Invested) / Invested,
           Gain = NAV - Invested)
  
  
  #### What do I own today? ####
  holdings_today = tail(daily_holdings, 1) %>%
    pivot_longer(!date, names_to = "ticker", values_to = "holdings")
  
  NAV_today = tail(daily_NAV, 1) %>%
    pivot_longer(!date, names_to = "ticker", values_to = "NAV")
  
  invested_today = tail(daily_invested, 1) %>%
    pivot_longer(!date, names_to = "ticker", values_to = "invested")
  
  # put it all together
  today = plyr::join_all(list(holdings_today,NAV_today,invested_today), 
                         by=c('date', 'ticker'), 
                         type='left') %>%
    mutate(gain = NAV - invested,
           return = gain / invested) %>%
    left_join(categorical, by = c(ticker = "instrument")) %>%
    select(-date)
  
  #### Compute Daily Data ####
  dailyData <- function(completedata){
    # full data by day
    completedata %>%
      select(ticker, date, `data.holdings`, adjClose, 
             `data.invested`, NAV, Return,
             `data.account`, `data.format`) %>%
      rename(holdings = `data.holdings`,
             invested = `data.invested`,
             account = `data.account`,
             format = `data.format`,
             nav = NAV,
             return = Return) %>%
      filter(holdings > 0) -> portDaily
    
    return(portDaily)
  }
  daily = dailyData(completedata)
  
  # calculate portfolio by day
  daily %>% group_by(date) %>% 
    summarise(nav = sum(nav, na.rm=T),
              invested = sum(invested, na.rm=T),
              return = (nav - invested) / invested) %>%
    mutate(ticker = "Porfolio",
           account = "Portfolio"
    ) -> portfolio
  
  # write to SQL database
  dbWriteTable(con, SQL(paste0(sheet, '.holdings_daily')), daily, overwrite=T)
  dbWriteTable(con, SQL(paste0(sheet, '.portfolio_daily')), portfolio, overwrite=T)
  dbWriteTable(con, SQL(paste0(sheet, '.journal')), daily, overwrite=T)
  
}

# clear out the environment
rm(list = ls())
