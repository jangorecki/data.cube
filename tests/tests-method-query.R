library(data.table)
library(data.cube)

options("datacube.jj" = FALSE)

X = populate_star(N = 1e5, surrogate.keys = FALSE)
ff = fact$new(x = X$fact$sales,
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
