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

# sorting of NA to last
r = rollup(cb, MARGIN = c("prod_name","geog_abb"), j = .(value = sum(value)))
rr = format(r)
stopifnot(is.data.table(rr), rr[nrow(rr)][is.na(prod_name) & is.na(geog_abb), .N==1L])

# format rollup same dimension on 2 attributes in bad order - normalize to not use surrogate key
r = rollup(cb, MARGIN = c("geog_abb", "geog_division_name"), j = .(value = sum(value)), normalize = FALSE)
stopifnot(
    is.data.table(r),
    identical(names(r), c("geog_abb","geog_division_name","level","value")),
    nrow(r)==101L,
    r[nrow(r)][is.na(geog_abb) & is.na(geog_division_name), .N==1L]
)

# same as above in right order
r = rollup(cb, MARGIN = c("geog_division_name","geog_abb"), j = .(value = sum(value)), normalize = FALSE)
stopifnot(
    is.data.table(r),
    identical(names(r), c("geog_division_name","geog_abb","level","value")),
    nrow(r)==60L,
    r[nrow(r)][is.na(geog_abb) & is.na(geog_division_name), .N==1L]
)

# using hierarchy
r = rollup(cb, MARGIN = c("geog_division_name","time_year"), j = .(value = sum(value)))
rr = format(r)
stopifnot(is.data.table(rr), rr[nrow(rr)][is.na(geog_division_name) & is.na(time_year), .N==1L], nrow(rr)==55L)

# reverse order of dims
r = rollup(cb, MARGIN = c("time_year","geog_division_name"), j = .(value = sum(value)))
rr = format(r)
stopifnot(is.data.table(rr), rr[nrow(rr)][is.na(geog_division_name) & is.na(time_year), .N==1L], nrow(rr)==51L)

# custom format per measure, see *currency* example from nice SO: http://stackoverflow.com/a/23833928/2490497
printCurrency = function(value, currency.sym="$", digits=2, sep=",", decimal=".", ...) paste(currency.sym, formatC(value, format = "f", big.mark = sep, digits=digits, decimal.mark=decimal), sep="")
stopifnot(printCurrency(123123.334)=="$123,123.33")
rcurrency = format(r, measure.format = list(value = printCurrency))
stopifnot(
    is.character(rcurrency$value),
    all(substr(rcurrency$value,1L,1L)=="$"),
    all.equal(as.numeric(gsub(",", "", gsub("$", "", rcurrency$value, fixed = TRUE))),format(r)$value)
)

# format levels of aggregates
r = rollup(cb, c("time_year","geog_region_name", "curr_type","prod_gear"), FUN = sum)
stopifnot(
    nrow(format(r[,,,,0L]))==120L,
    nrow(format(r[,,,,1L]))==5L,
    nrow(format(r[,,,,2L]))==20L,
    nrow(format(r[,,,,3L]))==40L,
    nrow(format(r[,,,,4L]))==1L
)

# all dims should have single columns as rollup was on highest aggregates
stopifnot(all(r$dapply(ncol)==1L))

# check normalization for time with surrogate key
r = rollup(cb, c("time_year","time_month","geog_division_name", "curr_name","prod_name"), FUN = sum)
stopifnot(
    identical(names(r$env$dims$time), c("time_id","time_month","time_month_name","time_quarter","time_quarter_name","time_year")),
    identical(names(r$env$fact$sales), c("time_id","geog_division_name","curr_name","prod_name","level","amount","value"))
)

# dcast
# as.data.table(r) # fix!
# r$dapply(key)
# format(r)
# rr = format(r, dcast = TRUE, formula = time_year ~ geog_region_name + curr_type)
# format(r)
# stopifnot(all.equal(dim(rr), c(4L,4L)), identical(names(rr), c("year","active","inactive","removed")))

# tests status ------------------------------------------------------------

invisible(FALSE)
