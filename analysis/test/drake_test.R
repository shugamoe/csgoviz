library(drake)
library(data.table)
library(ggplot2)

create_plot <- function(data){
  ggplot(data, aes(x = name)) +
    geom_histogram(stat = "count")
}

plan <- drake_plan(
  con = DBI::dbConnect(RPostgres::Postgres(), user = "gamevis", password = "gamevis"),
  raw_data = dbGetQuery(con, 'select * FROM events'),
  hist = create_plot(raw_data),
)
config <- drake_config(plan)
vis_drake_graph(config)


# Do real work
make(plan)
