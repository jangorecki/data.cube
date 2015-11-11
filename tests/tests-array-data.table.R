library(data.table)
library(data.cube)

X = populate_star(1e5, Y = 2015)
cb = as.cube(X)[,,,,as.Date("2015-01-15")] # drop time dimension to fit array into memory
dimnames = dimnames(cb)
measure = "value"
dt = as.data.table(cb)
ar = as.array(cb, measure = measure)
dimcols = c(product = "prod_name", customer = "cust_profile", currency = "curr_name", geography = "geog_abb")

# length of array results, dummy tests
stopifnot(
    prod(sapply(dimnames, length)) > 0L,
    dt[, prod(sapply(.SD, uniqueN)), .SDcols = dimcols] > 0L
)

## as.data.table
stopifnot(all.equal(dt[, c(dimcols, measure), with=FALSE], setnames(as.data.table(ar), names(dimcols), dimcols)))

## as.array

# use dimcols
stopifnot(all.equal(ar, as.array(dt, dimcols, measure)))

# use dimcols unnamed
ar.ren = ar
dimnames(ar.ren) = setNames(dimnames, dimcols)
stopifnot(all.equal(ar.ren, as.array(dt, unname(dimcols), measure)))

# use dimnames only
dt.ren = copy(dt)
setnames(dt.ren, dimcols, names(dimcols))
stopifnot(all.equal(ar, as.array(dt.ren, measure = measure, dimnames = dimnames)))

# use dimnames and dimcols
stopifnot(all.equal(ar, as.array(dt, dimcols = dimcols, measure = measure, dimnames = dimnames)))

# detect measure
stopifnot(
    all.equal(ar, as.array(dt[, c(dimcols, measure), with=FALSE], dimcols))
    , all.equal(ar, as.array(dt.ren[, c(names(dimcols), measure), with=FALSE], dimnames = dimnames))
    , all.equal(ar, as.array(dt[, c(dimcols, measure), with=FALSE], dimcols = dimcols, dimnames = dimnames))
)
