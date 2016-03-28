library(data.table)
library(data.cube)

set.seed(1L)
ar.dimnames = list(color = c("green","red","yellow"), # sorted dimnames
                   year = as.character(2011:2015),
                   status = c("active","archived","inactive","removed"))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)

dc = as.data.cube(ar)
dt = as.data.table(dc, na.fill = TRUE)
dimcols = setNames(nm = names(ar.dimnames))
measure = "value"
stopifnot(
    prod(sapply(ar.dimnames, length)) == 60L,
    dt[, prod(sapply(.SD, uniqueN)), .SDcols = dimcols] == 60L,
    all.equal(ar, as.array(dt, dimnames = ar.dimnames, measure = measure)),
    all.equal(dt, as.data.table(ar, na.rm = FALSE))
)

## as.data.table
stopifnot(all.equal(dt[, c(dimcols, measure), with=FALSE], setnames(as.data.table(ar, na.rm = FALSE), names(dimcols), dimcols)))

## as.array

# use dimcols
stopifnot(all.equal(ar, as.array(dt, dimcols, measure)))

# use dimcols unnamed
ar.ren = ar
dimnames(ar.ren) = setNames(ar.dimnames, dimcols)
stopifnot(all.equal(ar.ren, as.array(dt, unname(dimcols), measure)))

# use dimnames only
dt.ren = copy(dt)
setnames(dt.ren, dimcols, names(dimcols))
stopifnot(all.equal(ar, as.array(dt.ren, measure = measure, dimnames = ar.dimnames)))

# use dimnames and dimcols
stopifnot(all.equal(ar, as.array(dt, dimcols = dimcols, measure = measure, dimnames = ar.dimnames)))

# detect measure
stopifnot(
    all.equal(ar, as.array(dt[, c(dimcols, measure), with=FALSE], dimcols)),
    all.equal(ar, as.array(dt.ren[, c(names(dimcols), measure), with=FALSE], dimnames = ar.dimnames)),
    all.equal(ar, as.array(dt[, c(dimcols, measure), with=FALSE], dimcols = dimcols, dimnames = ar.dimnames))
)
