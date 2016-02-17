library(data.table)
library(data.cube)

# level ----

lvl = level$new(x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA), key = "a", properties = c("b","d"))
stopifnot(
    inherits(lvl, "level"),
    identical(lvl$key, "a"),
    identical(lvl$properties, c("b","d")),
    identical(dim(lvl$data), c(6L, 3L))
)

# hierarchy ----

hrh1 = hierarchy$new(x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA), key = "a", levels = list("b" = character(), "a" = c("b")))
hrh2 = hierarchy$new(x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA), key = "a", levels = list("d" = character(), "a" = c("d")))
stopifnot(TRUE) # TO DO
# sequence processing of levels in hierarchy from top to bottom keeping parent references

# dimension ----

dim = dimension$new(
    x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA),
    key = "a",
    hierarchies = list(
        list("b" = character(), "a" = c("b")),
        list("d" = character(), "a" = c("d"))
    )
)
stopifnot(
    identical(base::dim(dim$data), c(6L, 3L)),
    identical(dim$key, "a"),
    length(dim$hierarchies) == 2L,
    sapply(dim$hierarchies, function(x) length(x$levels)) == 2L,
    c(sapply(dim$hierarchies, function(x) names(x$levels))) == c("b","a","d","a")
)

# time dimension
X = populate_star(N = 1e5, surrogate.keys = FALSE)
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
stopifnot(
    inherits(time, "dimension"),
    names(time$hierarchies) == c("monthly","weekly"),
    time$hierarchies$monthly$key == "time_date",
    # all leafs in levels are normalized data.tables
    unlist(lapply(time$hierarchies, function(hrh) lapply(hrh$levels, function(lvl) is.data.table(lvl$data))))
)
sprintf("size of all tables in 'time' dimension: %.3f MB.", sum(unlist(lapply(time$hierarchies, function(hrh) lapply(hrh$levels, function(lvl) as.numeric(object.size(lvl$data))))))/(1024L^2L))
rm(X)

# measure ----

m = measure$new(x = "z", label = "score etc.")
m = measure$new(x = "z", label = "score etc.", fun.aggregate = "sum", na.rm = TRUE)
m$format()
stopifnot(TRUE) # TO DO

# fact ----

options("datacube.jj" = FALSE)
ff = fact$new(x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = 1:12*sin(1:12)),
              id.vars = c("a","b","d"),
              measure.vars = "z")
stopifnot(
    identical(dim(ff$data), c(6L, 4L))
)

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

# data.cube ----

X = populate_star(N = 1e5, surrogate.keys = FALSE)
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
ff = fact$new(x = X$fact$sales,
              id.vars = c("geog_abb","time_date"),
              measure.vars = c("amount","value"),
              fun.aggregate = "sum",
              na.rm = TRUE)

dc = data.cube$new(fact = ff, dimensions = list(time = time, geography = geog))
stopifnot(
    is.data.table(dc$fact$data),
    # Normalization
    identical(dim(dc$dimensions$geography$hierarchies[[1L]]$levels$geog_abb$data), c(50L, 4L)),
    identical(dim(dc$dimensions$geography$hierarchies[[1L]]$levels$geog_region_name$data), c(4L, 1L)),
    identical(dim(dc$dimensions$time$hierarchies[[1L]]$levels$time_month$data), c(12L, 2L)),
    identical(dim(dc$dimensions$time$hierarchies[[1L]]$levels$time_year$data), c(5L, 1L)),
    identical(dim(dc$dimensions$time$hierarchies[[1L]]$levels$time_date$data), c(1826L, 4L))
)

# data.cube$print ----

out = capture.output(print(dc))
stopifnot(
    out[1L] == "<data.cube>",
    out[2L] == "fact:",
    out[4L] == "dimensions:",
    length(out) == 7L
)

# data.cube$schema ----

dict = dc$schema()
stopifnot(
    nrow(dict) == 13L,
    c("type","name","hierarchy","level","nrow","ncol","mb","address") %in% names(dict),
    dict[type=="fact", .N] == 1L
)

# data.cube$query ----

#dc$query()

# `[.data.cube` ----

#dc[]

# `[[.data.cube` ----

#dc[[]]
