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

# sorting of NA to last
r = rollup(cb, MARGIN = c("color","year"), j = .(value = sum(value)))
rr = format(r)
stopifnot(is.data.table(rr), rr[nrow(rr)][is.na(color) & is.na(year), .N==1L])

# custom format per measure, see *currency* example from nice SO: http://stackoverflow.com/a/23833928/2490497
printCurrency = function(value, currency.sym="$", digits=2, sep=",", decimal=".", ...) paste(currency.sym, formatC(value, format = "f", big.mark = sep, digits=digits, decimal.mark=decimal), sep="")
stopifnot(printCurrency(123123.334)=="$123,123.33")
rcurrency = format(r, measure.format = list(value = printCurrency))
stopifnot(
    is.character(rcurrency$value),
    as.numeric(gsub("$", "", rcurrency$value, fixed = TRUE))==format(r)$value
)

# dcast 2D
r = cb["green"]
rr = format(r, dcast = TRUE, formula = year ~ status)
stopifnot(all.equal(dim(rr), c(4L,4L)), identical(names(rr), c("year","active","inactive","removed")))

# dcast 3D
r = cb[c("green","red"),,c("active","inactive")]
rr = format(r, dcast = TRUE, formula = year ~ status + color)
stopifnot(all.equal(dim(rr), c(5L,5L)), identical(names(rr), c("year","active_green","active_red","inactive_green","inactive_red")))

### hierarchy ---------------------------------------------------------------

cb = as.cube(populate_star(1e5))

invisible(FALSE)
