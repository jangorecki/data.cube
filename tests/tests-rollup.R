library(data.table)
library(data.cube)

### no hierarchy ----------------------------------------------------------

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")), 
                   year = as.character(2011:2015), 
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)

cb = as.cube(ar)

# all subtotals
r = rollup(cb, MARGIN = c("color","year"), FUN = sum, na.rm=TRUE)
stopifnot(
    identical(dim(r), c(3L,5L,3L)),
    all.equal(
        cb$env$fact[[1L]][, .(value=sum(value))],
        r$env$fact[[1L]][, .(value=sum(value) / nrow(r$env$dims$level))] # confirm expected double counting
    ),
    all.equal(unname(r$dapply(names, simplify = TRUE)), c("color","year","level")),
    as.data.table(r)[is.na(color) & is.na(year), .N==1L],
    as.data.table(r)[is.na(year), .N==uniqueN(color)]
)

# chosen subtotals
r = rollup(cb, MARGIN = c("color","year"), INDEX = c(0L,2L), FUN = sum, na.rm=TRUE)
stopifnot(
    identical(dim(r), c(3L,5L,2L)),
    all.equal(
        cb$env$fact[[1L]][, .(value=sum(value))],
        r$env$fact[[1L]][, .(value=sum(value) / nrow(r$env$dims$level))] # confirm expected double counting
    ),
    all.equal(unname(r$dapply(names, simplify = TRUE)), c("color","year","level")),
    as.data.table(r)[is.na(color) & is.na(year), .N==1L]
)

# array does not handle NA dim keys - which reflects aggregate, so
arr = as.array(r)
stopifnot(all(is.na(arr[,,2L])))

# `j` arg vs `FUN`
r = rollup(cb, MARGIN = c("color","year"), j = .(value = sum(value, na.rm=TRUE)))
stopifnot(all.equal(r, rollup(cb, MARGIN = c("color","year"), NULL, sum, na.rm=TRUE)))
stopifnot(
    identical(dim(r), c(3L,5L,3L)),
    all.equal(
        cb$env$fact[[1L]][, .(value=sum(value))],
        r$env$fact[[1L]][, .(value=sum(value) / nrow(r$env$dims$level))] # confirm expected double counting
    ),
    all.equal(unname(r$dapply(names, simplify = TRUE)), c("color","year","level")),
    as.data.table(r)[is.na(color) & is.na(year), .N==1L],
    as.data.table(r)[is.na(year), .N==uniqueN(color)]
)

# INDEX = 0L should match to capply and aggregate
r = rollup(cb, MARGIN = c("color","year"), INDEX = 0L, FUN = sum)
stopifnot(
    all.equal(r, capply(cb, c("color","year"), sum)),
    all.equal(r, aggregate(cb, c("color","year"), sum))
)

### hierarchy ---------------------------------------------------------------

cb = as.cube(populate_star(1e5))

# various aggregate levels from rollup
by = c("time_year","geog_region_name", "curr_type","prod_gear")
r = rollup(cb, by, FUN = sum)
# most granular level of aggregates, compare with format as `aggregate` drops dimensions
stopifnot(
    all.equal(format(r[,,,,0L]), format(aggregate(cb, by, FUN = sum))),
    all.equal(format(r[,,,,1L]), format(aggregate(cb, by[1L], FUN = sum))),
    all.equal(format(r[,,,,2L]), format(aggregate(cb, by[1:2], FUN = sum))),
    all.equal(format(r[,,,,3L]), format(aggregate(cb, by[1:3], FUN = sum))),
    all.equal(format(r[,,,,4L]), format(aggregate(cb, by[0L], FUN = sum)))
)

# rollup(cb, MARGIN = c("curr_name","geog_abb"), FUN = sum)
# rollup(cb, MARGIN = c("geog_region_name","time_year","time_quarter"), FUN = sum)

# tests status ------------------------------------------------------------

invisible(FALSE)
