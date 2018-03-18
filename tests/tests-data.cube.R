library(data.table)
library(data.cube)

# initialize ----

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

# print ----

out = capture.output(print(dc))
stopifnot(
    out[1L] == "<data.cube>",
    out[2L] == "fact:",
    out[4L] == "dimensions:",
    length(out) == 10L
)

# str ----

r = capture.output(str(dc))
stopifnot(
    all.equal(r, capture.output(dc$schema()))
)

# dimnames ----

r = dimnames(dc)
stopifnot(
    is.list(r),
    identical(names(r), c("product","customer","currency","geography","time")),
    !sapply(r, function(x) anyDuplicated(r))
)

# schema ----

dict = dc$schema()
stopifnot(
    nrow(dict) == 26L,
    c("type","name","entity","nrow","ncol","mb","address","sorted") %in% names(dict),
    dict[type=="fact", .N] == 1L
)

# head ----

r = dc$head()
stopifnot(
    identical(names(r), c("fact","dimensions")),
    is.data.table(r$fact),
    is.list(r$dimensions),
    identical(names(r$dimensions), c("product", "customer", "currency", "geography", "time")),
    sapply(lapply(r$dimensions, `[[`, "base"), nrow) == 6L,
    unlist(lapply(r$dimensions, function(x) sapply(x$levels, is.data.table)))
)

# format ----

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")),
                   year = as.character(2011:2015),
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE),
           unname(ar.dim),
           ar.dimnames)
dc = as.data.cube(ar)
print.equal = function(x, dt) {
    if (!is.array(x)) x = as.array(x)
    stopifnot(is.array(x), is.data.table(dt))
    # basic compare that works on data in tests, split columns
    xdim = dim(x)
    x = capture.output(print(x))
    dt = capture.output(print(dt))
    if (length(xdim) == 1L) {
        as.num = function(x, na.strings = "NA") {
            stopifnot(is.character(x))
            na = x %in% na.strings
            x[na] = 0
            x = as.numeric(x)
            x[na] = NA_real_
            x
        }
        x = lapply(strsplit(x, " +"), `[`, -1L)
        x[[2L]] = as.num(x[[2L]])
        dt = lapply(strsplit(dt, " +"), `[`, -1L)
        dt[[2L]] = as.num(dt[[2L]])    
    } else if (length(xdim) == 2L) {
        x = strsplit(x, " +")[-1L]
        x[[1L]] = c("", x[[1L]]) # match first line for below line processing
        x = lapply(x, `[`, -1L)
        dt = lapply(strsplit(dt, " +"), `[`, -1L)
    } else if (length(xdim) >= 3L) {
        stop(sprintf("higher dimensionality arrays doesn't print to tabular format, your array has %s dimensions", length(xdim)))
    }
    all.equal(x, dt)
}
stopifnot(
    print.equal( # drop=TRUE
        ar["green",,],
        format(dc["green"], na.fill=TRUE, dcast = TRUE, formula = year ~ status)
    ),
    print.equal( # drop=FALSE, first there is drop=T because print must be on 2D array already to have tabular format
        ar["green",,][as.character(c(2012:2014)),, drop=FALSE],
        format(dc["green"][as.character(c(2012:2014)),, drop=FALSE], na.fill=TRUE, dcast = TRUE, formula = year ~ status)
    ),
    print.equal( # 1D
        ar["green","2013",],
        format(dc["green","2013",], na.fill=T, dcast=T, formula = . ~ status)[,-1L,with=F] # remove `.` column
    ),
    print.equal( # 2D
        ar[,"2013",],
        format(dc[,"2013",], na.fill=T, dcast=T, formula = color ~ status)
    ),
    print.equal( # 2D
        ar[,,"active"],
        format(dc[,,"active"], na.fill=T, dcast=T, formula = color ~ year)
    ),
    print.equal( # 2D
        ar["green",c("2014","2015"),],
        format(dc["green",c("2014","2015")], na.fill=T, dcast=T, formula = year ~ status)
    )
)

# apply.data.cube ----

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")),
                   year = as.character(2011:2015),
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE),
           unname(ar.dim),
           ar.dimnames)
dc = as.data.cube(ar)
stopifnot( # apply using `[.data.cube` with integers
    # MARGIN=1L
    all.equal(
        r <- apply.data.cube(dc, 1L),
        dc[, `-`, `-`]
    ),
    all.equal(as.array(r, na.fill = 0), apply(ar, 1L, sum, na.rm=TRUE)),
    # MARGIN=2:3
    all.equal(
        r <- apply.data.cube(dc, -1L),
        dc[`-`]
    ),
    all.equal(as.array(r, na.fill = 0), apply(ar, 2:3, sum, na.rm=TRUE)),
    all.equal(r, dc[`-`,]),
    all.equal(r, dc[`-`,,]),
    # MARGIN=c(1L, 3L)
    all.equal(
        r <- apply.data.cube(dc, -2L),
        dc[, `-`]
    ),
    all.equal(as.array(r, na.fill = 0), apply(ar, c(1L,3L), sum, na.rm=TRUE)),
    all.equal(r, dc[, `-`,]),
    # MARGIN=3L
    all.equal(
        r <- apply.data.cube(dc, 3L),
        dc[`-`, `-`]
    ),
    all.equal(as.array(r, na.fill = 0), apply(ar, 3L, sum, na.rm=TRUE)),
    all.equal(r, dc[`-`, `-`,]),
    # MARGIN=integer()
    all.equal(
        r <- apply.data.cube(dc, integer()),
        dc[`-`, `-`, `-`]
    ),
    length(r) == 1L, # grand total
    all.equal(as.array(r, na.fill = 0), sum(ar, na.rm=TRUE)),
    # MARGIN=1:3
    all.equal(
        r <- apply.data.cube(dc, 1:3),
        dc[]
    ),
    all.equal(as.array(r, na.fill = 0), apply(ar, 1:3, sum, na.rm=TRUE)),
    # MARGIN=c("year", "status")
    all.equal(
        as.array(apply.data.cube(dc, c("year","status")), na.fill = 0),
        apply(ar, c("year","status"), sum, na.rm=TRUE)
    ),
    # MARGIN=c("color") # dim 1L, could be dropped
    all.equal(
        as.array(apply.data.cube(dc["green", drop=FALSE], "color"), na.fill = 0),
        apply(ar["green",,, drop=FALSE], "color", sum, na.rm=TRUE)
    )
)

dc = as.data.cube(ar, na.rm = FALSE) # keep NA's for below na.rm=T/F tests
stopifnot( # apply with new FUN
    # sum na.rm=FALSE
    all.equal(
        as.array(apply.data.cube(dc, "color", sum, na.rm=FALSE)),
        apply(ar, "color", sum, na.rm=FALSE)
    ),
    all.equal(
        as.array(apply.data.cube(dc, c("year","status"), sum, na.rm=FALSE)),
        apply(ar, c("year","status"), sum, na.rm=FALSE)
    ),
    # sum na.rm=TRUE
    all.equal(
        as.array(apply.data.cube(dc, "color", sum, na.rm=TRUE)),
        apply(ar, "color", sum, na.rm=TRUE)
    ),
    all.equal(
        as.array(apply.data.cube(dc, c("year","status"), sum, na.rm=TRUE)),
        apply(ar, c("year","status"), sum, na.rm=TRUE)
    ),
    # mean na.rm=FALSE
    all.equal(
        as.array(apply.data.cube(dc, "color", mean, na.rm=FALSE)),
        apply(ar, "color", mean, na.rm=FALSE)
    ),
    all.equal(
        as.array(apply.data.cube(dc, c("year","status"), mean, na.rm=FALSE)),
        apply(ar, c("year","status"), mean, na.rm=FALSE)
    ),
    # mean na.rm=TRUE
    all.equal(
        as.array(apply.data.cube(dc, "color", mean, na.rm=TRUE)),
        apply(ar, "color", mean, na.rm=TRUE)
    ),
    all.equal(
        as.array(apply.data.cube(dc, c("year","status"), mean, na.rm=TRUE)),
        apply(ar, c("year","status"), mean, na.rm=TRUE)
    ),
    # 3D aggr
    all.equal(
        as.array(apply.data.cube(dc, 1:3)),
        apply(ar, 1:3, sum)
    ),
    all.equal( # sum na.fill = 0
        as.array(apply.data.cube(dc, 1:3, sum, na.rm=TRUE), na.fill = 0),
        apply(ar, 1:3, sum, na.rm=TRUE)
    ),
    all.equal(
        as.array(apply.data.cube(dc, 1:3, mean)),
        apply(ar, 1:3, mean)
    ),
    identical( # mean na.fill = NaN - all.equal wouldn't catch the data type, see `all.equal(NA_real_, NaN)`
        as.array(apply.data.cube(dc, 1:3, mean, na.rm=TRUE), na.fill = NaN),
        apply(ar, 1:3, mean, na.rm=TRUE)
    )
)

# format measures ----

# TODO
# format(dc["green",.,], na.fill=TRUE)
# format(dc["green",,])[, .(value=sum(value)), status]
# format function
# dc$fact$measures$value$fun.format = currency.format
# format(dc[,, "BGN", c("MT","SD"), as.Date("2015-08-18")], 
#        na.fill=T, 
#        dcast=T, formula = year ~ status, 
#        measure.format = list(value = currency.format),
#        dots.format = list(value = list(currency.sym = " PLN", sym.align="right")))
# 
# format(dc["green",c("2014","2015")], measure.format = list(value = currency.format))

# names and length ----

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")),
                   year = as.character(2011:2015),
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE),
           unname(ar.dim),
           ar.dimnames)
dc = as.data.cube(ar)
stopifnot(
    identical(names(dc), c("color","year","status","value")),
    length(dc) == 30L
)
