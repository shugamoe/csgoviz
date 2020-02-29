library(drake)
library(workflowr)
library(data.table)
library(DBI)

source("code/preProcessEvents.R")

con  <- dbConnect(RPostgres::Postgres(), user = "csgoviz", password = "csgoviz")
