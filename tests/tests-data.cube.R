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

# [ ] subset array consistency 3D with drop: 3x5x4
set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")), 
                   year = as.character(2011:2015), 
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)
dc = as.data.cube(ar)
# 1xNxN
stopifnot(
    all.equal(dc["green", drop=FALSE], as.data.cube(ar["green",,, drop=FALSE])),
    all.equal(as.array(dc["green", drop=FALSE]), ar["green",,, drop=FALSE])
)
#dev
# arr = ar["green",,, drop=TRUE]
# dcr = dc["green", drop=TRUE]
# as.data.cube(arr)
stopifnot(
    #all.equal(dc["green", drop=TRUE], as.data.cube(ar["green",,, drop=TRUE])),
    all.equal(as.array(dc["green", drop=TRUE]), ar["green",,, drop=TRUE])
)
# # 1x1xN
# dc["green","2014"]
# ar["green","2014",]
# # 1x2xN
# dc["green",c("2013","2014")]
# ar["green",c("2013","2014"),]
# # 2x2xN
# dc[c("green","red"),c("2013","2014")]
# ar[c("green","red"),c("2013","2014"),]
# # 2x2x3
# dc[c("green","red"),c("2013","2014"),c("active","archived","inactive")]
# ar[c("green","red"),c("2013","2014"),c("active","archived","inactive")]

# hierarchy

# cb = as.cube(populate_star(1e3))
# dc = as.data.cube(populate_star(1e3, hierarchies = TRUE))

# DEV TODO

# slice keys
# cbr = cb["Mazda RX4"]
# dcr = dc$subset("Mazda RX4")
# all.equal(dcr, as.data.cube(cbr))
# all.equal(as.cube(dcr), cbr)
# slice two keys
# cbr = cb["Mazda RX4",,,"NY"]
# stopifnot(identical(names(dimnames(r)), c("customer","currency","time")))
# slice hierarchy
# cbr = cb[,,,.(geog_division_name = "East North Central")]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), dim(r)[4L]==5L)
# slice two hierarchies
# cbr = cb[,,,.(geog_division_name = "East North Central"), .(time_year = 2014L)]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), identical(dim(r)[4:5], c(5L, 365L)))

# # slice keys drop=F
# cbr = cb["Mazda RX4", drop=FALSE]
# dcr = dc["Mazda RX4", drop=FALSE]
# zz = as.cube(dcr)
# str(zz)
# str(cbr)
# zz$env$fact$fact
# cbr$env$fact$fact
# dcr$fact$data
# 
# all.equal(dcr, as.data.cube(cbr))
# all.equal(as.cube(dcr), cbr)
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), dim(r)[1L]==1L)
# slice two keys drop=F
# cbr = cb["Mazda RX4",,,"NY", drop=FALSE]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), identical(dim(r)[c(1L,4L)], c(1L,1L)))
# slice hierarchy drop=F - would not be dropped anyway
# cbr = cb[,,,.(geog_division_name = "East North Central"), drop=FALSE]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), dim(r)[4L]==5L)
# slice two hierarchies drop=F - would not be dropped anyway
# cbr = cb[,,,.(geog_division_name = "East North Central"), .(time_year = 2014L), drop=FALSE]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), identical(dim(r)[4:5], c(5L, 365L)))

# slice
# cbr = cb[c("Mazda RX4","Honda Civic")]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), dim(r)[1L]==2L)
# dice two, one hierarchy
# cbr = cb[c("Mazda RX4","Honda Civic"),,,.(geog_division_name = c("Mountain","Pacific"))]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), identical(dim(r)[c(1L,4L)], c(2L,13L)))

# dice drop=F - would not be dropped anyway
# cbr = cb[c("Mazda RX4","Honda Civic"), drop=FALSE]
# stopifnot(identical(names(dimnames(r)), c("product","customer","currency","geography","time")), dim(r)[1L]==2L)

# use own names in ...
# stopifnot(all.equal(
#     dc["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))],
#     dc[product = "Mazda RX4",
#        customer = .(),
#        currency = .(curr_type = "crypto"),
#        geography = .(),
#        time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# ))

# NULL subset
# stopifnot(
#     nrow(as.data.table(dc[NULL]))==0L
#     , nrow(as.data.table(dc[.(NULL)]))==0L
#     , nrow(as.data.table(dc[,,NULL,,NULL]))==0L
#     , nrow(as.data.table(dc[,,.(NULL),,.(NULL)]))==0L
#     , nrow(as.data.table(dc[,,,,.(time_year = 2014L, time_quarter_name = NULL)]))==0L
# )

# drop arg
# stopifnot(
#     length(dim(dc[]))==5L
#     , length(dim(dc["Mazda RX4"]))==4L
#     , length(dim(dc["Mazda RX4", drop=FALSE]))==5L
# )

# tests status ------------------------------------------------------------

invisible(FALSE)
