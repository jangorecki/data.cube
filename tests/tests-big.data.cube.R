
pkgs = c("Rserve","RSclient","big.data.table")
apkgs = sapply(pkgs, requireNamespace, quietly = TRUE)
print(apkgs)
if(!all(apkgs)) quit("no", status = 0)

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
ff = as.fact(x = rscl,
             id.vars = c("prod_name","cust_profile","curr_name","geog_abb","time_date"),
             measure.vars = c("amount","value"),
             fun.aggregate = "sum",
             na.rm = TRUE)
stopifnot(
    is.big.data.table(ff$data),
    inherits(ff, "fact"),
    sapply(ff$measures, inherits, "measure"),
    identical(ff$measure.vars, c("amount","value"))
)
print(ff$data)

# fact$query ----

# by = geog_abb
r = ff$query(by = "geog_abb", measure.vars = "amount")
stopifnot(
    is.data.table(r),
    c("geog_abb","amount") %in% names(r),
    nrow(r) == 50L
)

# i %in% 1:2, by = geog_abb
r = ff$query(i = geog_abb %in% c("ND","TX"), by = "geog_abb", measure.vars = c("value"))
stopifnot(
    is.data.table(r),
    c("geog_abb","value") %in% names(r),
    nrow(r) == 2L
)

# i = CJ(1:2, 1:3), by = .(geog_abb, time_date)
r = ff$query(i.dt = CJ(geog_abb = c("ND","TX"), time_date = as.Date(c("2010-01-01","2010-01-02","2010-01-03")), unique = TRUE), by = .(geog_abb, time_date))
stopifnot(
    is.data.table(r),
    c("geog_abb","time_date","amount","value") %in% names(r),
    nrow(r) == 3L
)

rscl.close(rscl)

# data.cube ----

X = populate_star(N = 1e5, surrogate.keys = FALSE)
time = as.dimension(X$dims$time,
                    id.vars = "time_date",
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
geog = as.dimension(X$dims$geography,
                    id.vars = "geog_abb",
                    hierarchies = list(
                        list(
                            "geog_region_name" = character(),
                            "geog_division_name" = c("geog_region_name"),
                            "geog_abb" = c("geog_name","geog_division_name","geog_region_name")
                        )
                    ))

rscl = rscl.connect(port = 33311:33314)
rscl.require(rscl, c("data.table","data.cube"))
bdt = as.big.data.table(X$fact$sales, rscl)
ff = as.fact(x = rscl,
             id.vars = c("geog_abb","time_date"),
             measure.vars = c("amount","value"),
             na.rm = TRUE)
rm(bdt)

dc = as.data.cube(ff, list(time = time, geography = geog))
stopifnot(
    is.big.data.table(dc$fact$data),
    # Normalization
    identical(dim(dc$dimensions$geography$levels$geog_abb$data), c(50L, 4L)),
    identical(dim(dc$dimensions$geography$levels$geog_region_name$data), c(4L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_month$data), c(12L, 2L)),
    identical(dim(dc$dimensions$time$levels$time_year$data), c(5L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_date$data), c(1826L, 5L)),
    all.equal(X$fact$sales[, .(value = sum(value))], dc$fact$data[, .(value = sum(value)), outer.aggregate = TRUE])
)
rscl.close(rscl)

# logR + data.cube ----

apkg = requireNamespace("logR", quietly = TRUE)
print(apkg)
if(apkg){
    
    library(logR)
    
    ## for check on localhost use postgres service
    # docker run --rm -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres --name pg-data.cube postgres:9.5
    if(logR::logR_connect()){
        stopifnot(logR::logR_schema(drop = TRUE))
        on = options("bigdatatable.log" = TRUE, "logR.nano.debug" = TRUE)
        
        # connect
        
        X = populate_star(N = 1e5, surrogate.keys = FALSE)
        rscl = big.data.table::rscl.connect(port = 33311:33314)
        stopifnot(rscl.require(rscl, c("data.table", "logR")))
        stopifnot(rscl.eval(rscl, logR::logR_connect(quoted = TRUE), lazy = FALSE))
        bdt = as.big.data.table(X$fact$sales, rscl)
        ff = as.fact(x = rscl,
                     id.vars = c("geog_abb","time_date"),
                     measure.vars = c("amount","value"),
                     na.rm = TRUE)
        r = bdt[1L]
        # TO DO: add data.cube `[` queries when ready
        options(on)
        lr = logR::logR_dump()
        stopifnot(
            is.data.table(r),
            nrow(r) == 4L,
            is.big.data.table(ff$data),
            nrow(lr) == 10L,
            lr$status == "success",
            all.equal(lr[, .(logr_id, parent_id)], data.table(logr_id = 1:10, parent_id = c(NA, rep(1L, 4L), NA, rep(6L, 4L)))),
            lr[7:10, expr] == "x[1L]"
        )
        rm(bdt)
        # data.cube queries # TO DO
        
        on = options("datatable.prettyprint.char" = 80L)
        print(lr[])
        options(on)
        
        rscl.close(rscl)
    }
}

# shutdown nodes ----

library(RSclient)

port = 33311:33314

# shutdown any running nodes
l = lapply(setNames(port, port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(l, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))

invisible(TRUE)
