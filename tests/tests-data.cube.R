library(data.table)
library(data.cube)

# initialize ----

X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)
dims = lapply(setNames(seq_along(X$dims), names(X$dims)), function(i){
    as.dimension(X$dims[[i]],
                 key = key(X$dims[[i]]),
                 hierarchies = X$hierarchies[[i]])
})
ff = as.fact(x = X$fact$sales,
             id.vars = key(X$fact$sales),
             measure.vars = c("amount","value"),
             fun.aggregate = "sum",
             na.rm = TRUE)
dc = as.data.cube(ff, dims)

stopifnot(
    is.data.table(dc$fact$data),
    # Normalization
    identical(dim(dc$dimensions$geography$levels$geog_abb$data), c(50L, 4L)),
    identical(dim(dc$dimensions$geography$levels$geog_region_name$data), c(4L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_month$data), c(12L, 2L)),
    identical(dim(dc$dimensions$time$levels$time_year$data), c(5L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_date$data), c(1826L, 6L))
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

# subset ----

# [x] subset array consistency 3D with drop: 3x5x4
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
    ## # 1xNxN
    # drop=FALSE
    all.equal(dc["green", drop=FALSE], as.data.cube(ar["green",,, drop=FALSE])),
    all.equal(as.array(dc["green", drop=FALSE]), ar["green",,, drop=FALSE]),
    # drop-TRUE
    all.equal(dc["green", drop=TRUE], as.data.cube(ar["green",,, drop=TRUE])),
    all.equal(as.array(dc["green", drop=TRUE]), ar["green",,, drop=TRUE]),
    ## 1x1xN
    # drop=FALSE
    all.equal(dc["green","2015", drop=FALSE], as.data.cube(ar["green","2015",, drop=FALSE])),
    all.equal(as.array(dc["green","2015", drop=FALSE]), ar["green","2015",, drop=FALSE]),
    # drop-TRUE
    all.equal(dc["green","2015", drop=TRUE], as.data.cube(ar["green","2015",, drop=TRUE], dims="status")), # undrop dim name because `drop=T` reduces to vector
    all.equal(as.array(dc["green","2015", drop=TRUE]), ar["green","2015",, drop=TRUE]),
    ## 1x2xN
    # drop=FALSE
    all.equal(dc["green",c("2012","2013"), drop=FALSE], as.data.cube(ar["green",c("2012","2013"),, drop=FALSE])),
    all.equal(as.array(dc["green",c("2012","2013"), drop=FALSE]), ar["green",c("2012","2013"),, drop=FALSE]),
    # drop-TRUE
    all.equal(dc["green",c("2012","2013"), drop=TRUE], as.data.cube(ar["green",c("2012","2013"),, drop=TRUE])),
    all.equal(as.array(dc["green",c("2012","2013"), drop=TRUE]), ar["green",c("2012","2013"),, drop=TRUE]),
    ## 2x2xN
    # drop=FALSE
    all.equal(dc[c("green","red"),c("2012","2013"), drop=FALSE], as.data.cube(ar[c("green","red"),c("2012","2013"),, drop=FALSE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"), drop=FALSE]), ar[c("green","red"),c("2012","2013"),, drop=FALSE]),
    # drop-TRUE
    all.equal(dc[c("green","red"),c("2012","2013"), drop=TRUE], as.data.cube(ar[c("green","red"),c("2012","2013"),, drop=TRUE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"), drop=TRUE]), ar[c("green","red"),c("2012","2013"),, drop=TRUE]),
    ## # 2x2x3
    # drop=FALSE
    all.equal(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE], as.data.cube(ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE]), ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE]),
    # drop-TRUE
    all.equal(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE], as.data.cube(ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE]), ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE])
)
stopifnot( # NULL subset
    nrow(as.data.table(dc[NULL]))==0L
    , nrow(as.data.table(dc[.(NULL)]))==0L
    , nrow(as.data.table(dc[NULL,,NULL]))==0L
    , nrow(as.data.table(dc[,NULL,.(NULL)]))==0L
    , identical(dimnames(dc[,NULL,.(NULL)]), list(color = c("green","red","yellow"))) # inconsistency to base::array see: http://stackoverflow.com/q/36242181/2490497
)

# - [x] subset hierarchy consistency to old `cube`

X = populate_star(1e3, hierarchies = TRUE)
cb = as.cube(X)
dc = as.data.cube(X)
stopifnot( # slice single values
    ## drop=TRUE
    # slice keys
    all.equal(dc["Mazda RX4"], as.data.cube(cb["Mazda RX4"], hierarchies = X$hierarchies[-1L])), # exclude dropped product dim
    # slice two keys
    all.equal(r <- dc["Mazda RX4",,,"NY"], as.data.cube(cb["Mazda RX4",,,"NY"], hierarchies = X$hierarchies[-c(1L,4L)])),
    identical(names(dimnames(r)), c("customer","currency","time")),
    # slice hierarchy
    all.equal(r <- dc[,,,.(geog_division_name = "East North Central")], as.data.cube(cb[,,,.(geog_division_name = "East North Central")], hierarchies = X$hierarchies)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    dim(r)[4L]==5L,
    # slice two hierarchies
    all.equal(r <- dc[,,,.(geog_division_name = "East North Central"), .(time_year = 2014L)], as.data.cube(cb[,,,.(geog_division_name = "East North Central"), .(time_year = 2014L)], hierarchies = X$hierarchies)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    identical(dim(r)[4:5], c(5L, 365L)),
    ## drop=FALSE
    # slice keys, * we need to drop indexes afterward with $serindex(drop=TRUE)
    all.equal(dc["Mazda RX4", drop=FALSE]$setindex(TRUE), as.data.cube(cb["Mazda RX4", drop=FALSE], hierarchies = X$hierarchies)$setindex(TRUE)), # exclude dropped product dim
    # slice two keys
    all.equal(r <- dc["Mazda RX4",,,"NY", drop=FALSE], as.data.cube(cb["Mazda RX4",,,"NY", drop=FALSE], hierarchies = X$hierarchies)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    # slice hierarchy
    all.equal(r <- dc[,,,.(geog_division_name = "East North Central"), drop=FALSE], as.data.cube(cb[,,,.(geog_division_name = "East North Central"), drop=FALSE], hierarchies = X$hierarchies)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    dim(r)[4L]==5L,
    # slice two hierarchies
    all.equal(r <- dc[,,,.(geog_division_name = "East North Central"), .(time_year = 2014L), drop=FALSE], as.data.cube(cb[,,,.(geog_division_name = "East North Central"), .(time_year = 2014L), drop=FALSE], hierarchies = X$hierarchies)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    identical(dim(r)[4:5], c(5L, 365L))
)
stopifnot( # multi value
    # drop=TRUE
    all.equal(r <- dc[c("Mazda RX4","Honda Civic")]$setindex(TRUE), 
              as.data.cube(cb[c("Mazda RX4","Honda Civic")], hierarchies = X$hierarchies)$setindex(TRUE)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    dim(r)[1L]==2L,
    # dice two, one hierarchy
    all.equal(r <- dc[c("Mazda RX4","Honda Civic"),,,.(geog_division_name = c("Mountain","Pacific"))], 
              as.data.cube(cb[c("Mazda RX4","Honda Civic"),,,.(geog_division_name = c("Mountain","Pacific"))], hierarchies = X$hierarchies)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    identical(dim(r)[c(1L,4L)], c(2L,13L)),
    # drop=FALSE - would not be dropped anyway
    all.equal(r <- dc[c("Mazda RX4","Honda Civic"), drop=FALSE]$setindex(TRUE), 
              as.data.cube(cb[c("Mazda RX4","Honda Civic"), drop=FALSE], hierarchies = X$hierarchies)$setindex(TRUE)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    dim(r)[1L]==2L,
    all.equal(r <- dc[c("Mazda RX4","Honda Civic"),,,.(geog_division_name = c("Mountain","Pacific")), drop=FALSE], 
              as.data.cube(cb[c("Mazda RX4","Honda Civic"),,,.(geog_division_name = c("Mountain","Pacific")), drop=FALSE], hierarchies = X$hierarchies)),
    identical(names(dimnames(r)), c("product","customer","currency","geography","time")),
    identical(dim(r)[c(1L,4L)], c(2L, 13L))
)
stopifnot(all.equal( # use own names in ...
    dc["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))],
    dc[product = "Mazda RX4",
       customer = .(),
       currency = .(curr_type = "crypto"),
       geography = .(),
       time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
))
stopifnot( # NULL subset
    nrow(as.data.table(dc[NULL]))==0L
    , nrow(as.data.table(dc[.(NULL)]))==0L
    , nrow(as.data.table(dc[,,NULL,,NULL]))==0L
    , nrow(as.data.table(dc[,,.(NULL),,.(NULL)]))==0L
    , nrow(as.data.table(dc[,,,,.(time_year = 2014L, time_quarter_name = NULL)]))==0L
)
# drop arg
stopifnot(
    length(dim(dc[]))==5L
    , length(dim(dc["Mazda RX4"]))==4L
    , length(dim(dc["Mazda RX4", drop=FALSE]))==5L
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

# set.seed(1L)
# dc = as.data.cube(populate_star(1e5, Y = 2015L, hierarchies = TRUE, seed=123))
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
