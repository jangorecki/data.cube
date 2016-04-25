library(data.table)
library(data.cube)

## basic non hierarchical data

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")),
                   year = as.character(2011:2015),
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE),
           unname(ar.dim),
           ar.dimnames)
dc = as.data.cube(ar)

# filter dimensions `.()` ----

# collapse dimensions `+()` ----

stopifnot( # apply while filter
    # i=1L - MARGIN=3L
    all.equal(
        r <- apply.data.cube(dc["green", drop=FALSE], 3L),
        dc["green", .]
    ),
    all.equal(as.array(r, na.fill = 0), apply(ar["green",,, drop=FALSE], 3L, sum, na.rm=TRUE)),
    all.equal(r, dc["green", .,]),
    # i=3L - MARGIN=c(1L,3L)
    all.equal(
        r <- apply.data.cube(dc[,, c("active","inactive"), drop=FALSE], c(1L,3L)),
        dc[, ., c("active","inactive")]
    ),
    all.equal(as.array(r, na.fill = 0), apply(ar[,, c("active","inactive"), drop=FALSE], c(1L,3L), sum, na.rm=TRUE))
)
stopifnot( # drop=T/F
    all.equal( # grand total after filter drop=TRUE
        r <- dc["green", ., .],
        apply.data.cube(dc["green"], character())
    ),
    length(r) == 1L, # grand total
    identical(dim(r), integer(0)),
    all.equal(as.array(r), sum(ar["green",,], na.rm=TRUE)),
    all.equal( # drop=FALSE
        r <- dc["green", ., ., drop=FALSE],
        apply.data.cube(dc["green", drop=FALSE], 1L)
    ),
    identical(dim(r), 1L),
    all.equal(as.array(r), apply(ar["green",,, drop=FALSE], 1L, sum, na.rm=TRUE))
)

# collapse dimensions `+()` with filtering ----

# rollup dimensions `-()` ----

# rollup dimensions `-()` with filtering ----

## multidimensional hierarchical data ----

X = populate_star(N = 1e3, surrogate.keys = FALSE)
dims = lapply(setNames(seq_along(X$dims), names(X$dims)), function(i){
    as.dimension(X$dims[[i]],
                 key = key(X$dims[[i]]),
                 hierarchies = X$hierarchies[[i]])
})
ff = as.fact(x = X$fact$sales,
             id.vars = key(X$fact$sales),
             measure.vars = c("amount","value"),
             fun.aggregate = sum,
             na.rm = TRUE)
dc = as.data.cube(ff, dims)

stopifnot( # expected dimensionality from as.data.table
    # .fact
    identical(dim(as.data.table(dc$fact)), c(1000L, 7L)),
    # .dimension
    identical(dim(as.data.table(dc$dimensions$geography)), c(50L, 4L)),
    identical(dim(as.data.table(dc$dimensions$time)), c(1826L, 8L)),
    # .levels
    identical(dim(as.data.table(dc$dimensions$geography$levels$geog_abb)), c(50L, 4L)),
    identical(dim(as.data.table(dc$dimensions$geography$levels$geog_region_name)), c(4L, 1L)),
    identical(dim(as.data.table(dc$dimensions$time$levels$time_month)), c(12L, 2L)),
    identical(dim(as.data.table(dc$dimensions$time$levels$time_year)), c(5L, 1L)),
    identical(dim(as.data.table(dc$dimensions$time$levels$time_date)), c(1826L, 6L))
)

# filter dimensions `.()` ----

