
# start nodes ----

library(Rserve)

port = 33311:33314

# shutdown any running nodes
rscl = lapply(setNames(port, port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(rscl, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))

# start cluster
invisible(sapply(port, function(port) Rserve(debug = FALSE, port = port, args = c("--no-save"))))

options("bigdatatable.log" = FALSE) # for interactive running tests
invisible(TRUE)
Sys.sleep(3)

# fact ----

library(data.table)
library(big.data.table)
library(data.cube)

rscl = big.data.table::rscl.connect(port = 33311:33314)
rscl.require(rscl, c("data.table","data.cube"))
X = populate_star(N = 1e5, surrogate.keys = FALSE)
bdt = as.big.data.table(X$fact$sales, rscl)
ff = fact$new(x = rscl,
              id.vars = c("prod_name","cust_profile","curr_name","geog_abb","time_date"),
              measure.vars = c("amount","value"),
              na.rm = TRUE)
stopifnot(
    is.big.data.table(ff$data),
    inherits(ff, "fact"),
    sapply(ff$measures, inherits, "measure"),
    identical(ff$measure.vars, c("amount","value"))
)
rscl.close(rscl)

# data.cube ----

X = data.cube::populate_star(N = 1e5, surrogate.keys = FALSE)
time = dimension$new(X$dims$time,
                     key = "time_date",
                     hierarchies = list(
                         "monthly" = list(
                             "time_year" = character(),
                             "time_quarter" = c("time_quarter_name"),
                             "time_month" = c("time_month_name"),
                             "time_date" = c("time_month","time_quarter","time_year")
                         ),
                         "weekly" = list(
                             "time_year" = character(),
                             "time_week" = character(),
                             "time_date" = c("time_week","time_year")
                         )
                     ))
geog = dimension$new(X$dims$geography,
                     key = "geog_abb",
                     hierarchies = list(
                         list(
                             "geog_region_name" = character(),
                             "geog_division_name" = character(),
                             "geog_abb" = c("geog_name","geog_division_name","geog_region_name")
                         )
                     ))

rscl = big.data.table::rscl.connect(port = 33311:33314)
rscl.require(rscl, c("data.table","data.cube"))
bdt = as.big.data.table(X$fact$sales, rscl)
ff = fact$new(x = rscl,
              id.vars = c("geog_abb","time_date"),
              measure.vars = c("amount","value"),
              na.rm = TRUE)
rm(bdt)

dc = data.cube$new(fact = ff, dimensions = list(time = time, geography = geog))
stopifnot(
    is.big.data.table(dc$fact$data),
    # Normalization
    identical(dim(dc$dimensions$geography$hierarchies[[1L]]$levels$geog_abb$data), c(50L, 4L)),
    identical(dim(dc$dimensions$geography$hierarchies[[1L]]$levels$geog_region_name$data), c(4L, 1L)),
    identical(dim(dc$dimensions$time$hierarchies[[1L]]$levels$time_month$data), c(12L, 2L)),
    identical(dim(dc$dimensions$time$hierarchies[[1L]]$levels$time_year$data), c(5L, 1L)),
    identical(dim(dc$dimensions$time$hierarchies[[1L]]$levels$time_date$data), c(1826L, 4L)),
    all.equal(X$fact$sales[, .(value = sum(value))], dc$fact$data[, .(value = sum(value)), outer.aggregate = TRUE])
)
rscl.close(rscl)

# shutdown nodes ----

library(RSclient)

port = 33311:33314

# shutdown any running nodes
l = lapply(setNames(port, port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(l, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))

invisible(TRUE)
