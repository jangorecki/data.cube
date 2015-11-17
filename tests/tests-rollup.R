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

# format cube - sorting of NA to last and custom format per measure, see *currency* example
rr = format(r)
stopifnot(is.data.table(rr), rr[nrow(rr)][is.na(color) & is.na(year), .N==1L])
# format currency from nice SO: http://stackoverflow.com/a/23833928/2490497
printCurrency = function(value, currency.sym="$", digits=2, sep=",", decimal=".", ...) paste(currency.sym, formatC(value, format = "f", big.mark = sep, digits=digits, decimal.mark=decimal), sep="")
stopifnot(printCurrency(123123.334)=="$123,123.33")
rcurrency = format(r, measure.format = list(value = printCurrency))
stopifnot(
    is.character(rcurrency$value),
    as.numeric(gsub("$", "", rcurrency$value, fixed = TRUE))==format(r)$value
)

# INDEX = 0L should match to capply and aggregate
r = rollup(cb, MARGIN = c("color","year"), INDEX = 0L, FUN = sum)
stopifnot(
    all.equal(r, capply(cb, c("color","year"), sum)),
    all.equal(r, aggregate(cb, c("color","year"), sum))
)

### hierarchy ---------------------------------------------------------------

# cb = as.cube(populate_star(1e5))
# 
# rollup(cb, MARGIN = c("curr_name","geog_abb"), FUN = sum)
# rollup(cb, MARGIN = c("geog_region_name","time_year","time_quarter"), FUN = sum)

invisible(FALSE)
