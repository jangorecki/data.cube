library(data.table)
library(data.cube)

# data.cube: 2 dimensions, produce hierarchies ----

X = populate_star(N = 1e3, surrogate.keys = FALSE)
time.hierarchies = list(
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
)
geog.hierarchies = list(list(
    "geog_region_name" = character(),
    "geog_division_name" = character(),
    "geog_abb" = c("geog_name","geog_division_name","geog_region_name")
))

dimensions = list(
    time = as.dimension(X$dims$time, key = "time_date", hierarchies = time.hierarchies),
    geography = as.dimension(X$dims$geography, key = "geog_abb", hierarchies = geog.hierarchies)
)

facts = as.fact(X$fact$sales,
                id.vars = c("geog_abb","time_date"),
                measure.vars = c("amount","value"),
                fun.aggregate = "sum",
                na.rm = TRUE)
dc = as.data.cube(facts, dimensions)

stopifnot(
    is.data.cube(dc)
)

# data.cube: 5 dimensions, hierarchies from `populate_star` ----

X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)
dims = lapply(setNames(seq_along(X$dims), names(X$dims)),
              function(i) as.dimension(X$dims[[i]], hierarchies = X$hierarchies[[i]]))
ff = as.fact(x = X$fact$sales,
             id.vars = key(X$fact$sales),
             measure.vars = c("amount","value"),
             fun.aggregate = "sum",
             na.rm = TRUE)
dc.f = as.data.cube(ff, dims)
dc.x = as.data.cube(X)
cb.x = as.cube(X)

stopifnot(
    is.data.cube(dc.f),
    is.data.cube(dc.x),
    all.equal(dc.x, as.data.cube(cb.x, hierarchies = X$hierarchies))
)

# in
dt = dc.f$denormalize()
# z = as.cube(dt, dims = lapply(X$dims, names))
dc.d = as.data.cube(dt,
                    id.vars = c("prod_name","cust_profile","curr_name","geog_abb","time_date"),
                    measure.vars = c("amount","value"),
                    dims = names(X$dims), 
                    hierarchies = X$hierarchies)
# stopifnot(
#     all.equal(dc.f, dc.d)
# )
# dc.f$id.vars
# 
# 
# # out
# stopifnot(
#     all.equal(X, as.list(dc.f)),
#     all.equal(dt, as.data.table(dc.d))
# )

# array: 3 dimensions ----

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")), 
                   year = as.character(2011:2015), 
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)

# in
cb.ar = as.cube(ar)
dc.ar = as.data.cube(ar)
stopifnot(all.equal(dim(cb.ar), dim(dc.ar)))

# out
stopifnot(
    all.equal(ar, as.array(cb.ar)),
    all.equal(ar, as.array(dc.ar)),
    all.equal(as.data.table(cb.ar), as.data.table(dc.ar)),
    nrow(as.data.table(dc.ar, na.fill = TRUE)) > nrow(as.data.table(dc.ar, na.fill = FALSE)),
    all.equal(dc.ar, as.data.cube(cb.ar))
)

# zero dimension fact
# r = aggregate(dc.ar, character(), FUN = sum)
# stopifnot(
#     nrow(format(r))==1L,
#     all.equal(format(r), as.data.table(r)),
#     all.equal(format(r)$value, as.array(r))
# )
