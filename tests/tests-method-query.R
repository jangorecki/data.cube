library(data.table)
library(data.cube)

options("datacube.jj" = FALSE)

# fact$initialize ----

X = populate_star(N = 1e3, surrogate.keys = FALSE)
ff = as.fact(x = X$fact$sales,
             id.vars = c("prod_name","cust_profile","curr_name","geog_abb","time_date"),
             measure.vars = c("amount","value"),
             na.rm = TRUE)
stopifnot(
    is.data.table(ff$data),
    sapply(ff$measures, inherits, "measure")
)

# fact$query ----

# by = geog_abb
r = ff$query(by = "geog_abb", measure.vars = c("amount"))
stopifnot(
    is.data.table(r),
    c("geog_abb","amount") %in% names(r),
    nrow(r) == 50L
)

# i %in% 1:2, by = geog_abb
r = ff$query(i = geog_abb %in% c("VT","AL"), by = "geog_abb", measure.vars = c("value"))
stopifnot(
    is.data.table(r),
    c("geog_abb","value") %in% names(r),
    nrow(r) == 2L
)

# i = CJ(1:3, 1:3), by = .(geog_abb, time_date)
r = ff$query(i.dt = CJ(geog_abb = c("VT","AL","TX"), time_date = as.Date(c("2010-01-01","2010-01-02","2010-01-03")), unique = TRUE), by = .(geog_abb, time_date))
stopifnot(
    is.data.table(r),
    c("geog_abb","time_date","amount","value") %in% names(r),
    nrow(r) == 3L
)

# data.cube$initialize ----

X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)

dims = lapply(setNames(seq_along(X$dims), names(X$dims)), function(i){
    as.dimension(X$dims[[i]],
                 id.vars = key(X$dims[[i]]),
                 hierarchies = X$hierarchies[[i]])
})
ff = as.fact(x = X$fact$sales,
             id.vars = sapply(dims, `[[`, "id.vars"),
             measure.vars = c("amount","value"),
             fun.aggregate = sum,
             na.rm = TRUE)
dc = as.data.cube(ff, dims)

# data.cube$subset ----

# dc[,,"BTC", c("TX","NY"), .(year = 2010L)]
# dc[,,"BTC", c("TX","NY")] # 4 args
# r = dc$subset()
# stopifnot(
#     TRUE
# )
