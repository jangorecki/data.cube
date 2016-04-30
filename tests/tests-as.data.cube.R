library(data.table)
library(data.cube)

# data.cube: 2 dimensions, produce hierarchies ----

X = populate_star(N = 1e3, surrogate.keys = FALSE)
time.hierarchies = list( # 2 hierarchies in time dimension
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
geog.hierarchies = list( # 1 hierarchy in geography dimension
    list(
        "geog_region_name" = character(),
        "geog_division_name" = c("geog_region_name"),
        "geog_abb" = c("geog_name","geog_division_name","geog_region_name")
    )
)

dimensions = list(
    time = as.dimension(X$dims$time, 
                        id.vars = "time_date", 
                        hierarchies = time.hierarchies),
    geography = as.dimension(X$dims$geography, 
                             id.vars = "geog_abb", 
                             hierarchies = geog.hierarchies)
)

facts = as.fact(X$fact$sales,
                id.vars = c("geog_abb","time_date"),
                measure.vars = c("amount","value"),
                fun.aggregate = sum,
                na.rm = TRUE)
dc = as.data.cube(facts, dimensions)

stopifnot(
    is.data.cube(dc),
    identical(names(dc), c("geog_abb","time_date","amount","value")) # order of dimensions aligned to facts
)

# data.cube: 5 dimensions, hierarchies from `populate_star` ----

X = populate_star(N = 1e3, surrogate.keys = FALSE)
dims = lapply(setNames(seq_along(X$dims), names(X$dims)),
              function(i) as.dimension(X$dims[[i]], hierarchies = X$hierarchies[[i]]))
ff = as.fact(x = X$fact$sales,
             id.vars = key(X$fact$sales),
             measure.vars = c("amount","value"),
             fun.aggregate = sum,
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
dc.d = as.data.cube(
    x = dt,
    id.vars = c("prod_name","cust_profile","curr_name","geog_abb","time_date"),
    measure.vars = c("amount","value"),
    fun.aggregate = sum,
    na.rm = TRUE,
    dims = c("product","customer","currency","geography","time"), 
    hierarchies = X$hierarchies
)
stopifnot(
    is.data.cube(dc.d),
    # all.equal(dc.f, dc.d) # cannot be equal because dc.d has already dropped unused dimension values, so there is a different subset of date
    all.equal(dc.f$fact, dc.d$fact),
    all.equal(names(dc.f$id.vars), names(dc.d$id.vars)),
    all.equal(names(dc.f$dimensions), names(dc.d$dimensions))
)

# out
stopifnot(
    all.equal(dt, as.data.table(dc.d))
)

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

# zero length objects ----

stopifnot(
    is.fact(as.fact(NULL)),
    is.dimension(as.dimension(NULL)),
    is.fact(as.fact(data.table(value = 10)))
)

# null fact
ff = as.fact(NULL)
# null.ff no dimensions, zero fact, zero dim
dc1 = suppressWarnings(as.data.cube(ff, list())) # warn that dimensions dropped
# null.ff 3 null.dd, zero fact, 3 null dim
dc2 = suppressWarnings(as.data.cube(ff, lapply(1:3, function(i) as.dimension(NULL)))) # warn that dimensions dropped
stopifnot(
    # dc1
    length(dc1$dimensions) == 0L,
    all.equal(dim(dc1), integer()),
    identical(dim(dc1$fact$data), c(0L,0L)),
    all.equal(unname(sapply(dc1$dimensions, dim)), list()),
    # dc2
    length(dc2$dimensions) == 0L,
    all.equal(dim(dc2), integer()),
    identical(dim(dc2$fact$data), c(0L,0L)),
    all.equal(unname(sapply(dc2$dimensions, dim)), list())
)

# 1row.ff no dimensions, 1 row fact, zero dim, fact as grand total
ff = as.fact(data.table(value = 10:11))
# null.ff no dimensions, 1 row fact, zero dim, fact as grand total
dc1 = as.data.cube(ff, list())
# null.ff 3 null.dd, 1 row fact, 3 null dim, fact as grand total
dc2 = suppressWarnings(as.data.cube(ff, lapply(1:3, function(i) as.dimension(NULL)))) # warn that dimensions dropped
stopifnot(
    # dc1
    length(dc1$dimensions) == 0L,
    all.equal(dim(dc1), integer()),
    identical(dim(dc1$fact$data), c(1L,1L)),
    all.equal(unname(sapply(dc1$dimensions, dim)), list()),
    # dc2
    length(dc2$dimensions) == 0L,
    all.equal(dim(dc2), integer()),
    identical(dim(dc2$fact$data), c(1L,1L)),
    all.equal(unname(sapply(dc2$dimensions, dim)), list())
)
