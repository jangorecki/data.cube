library(data.table)
library(data.cube)

## - [x] basic non hierarchical data ----

# - [x] order of dimnames is not retained
set.seed(1L)
ar.dimnames = list(color = c("green","yellow","red"), # unordered
                   year = sample(as.character(2011:2015)),
                   status = c("active","inactive","archived","removed"))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE),
           unname(ar.dim),
           ar.dimnames)
dc = as.data.cube(ar)
stopifnot(
    # no match
    !identical(dimnames(ar), dimnames(dc)),
    # validate match when sorted
    identical(lapply(dimnames(ar), sort), dimnames(dc))
)

# - [x] ordered data to proceed tests with
set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")),
                   year = as.character(2011:2015),
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE),
           unname(ar.dim),
           ar.dimnames)
dc = as.data.cube(ar)
stopifnot(identical(dimnames(ar), dimnames(dc))) # match because it was sorted

# - [x] filter dimension array style ----

# - [x] subset array consistency 3D with drop: 3x5x4
stopifnot(
    ## # 1xNxN
    # drop=FALSE
    all.equal(dc["green", drop=FALSE], as.data.cube(ar["green",,, drop=FALSE])),
    all.equal(as.array(dc["green", drop=FALSE]), ar["green",,, drop=FALSE]),
    # drop=TRUE
    all.equal(dc["green", drop=TRUE], as.data.cube(ar["green",,, drop=TRUE])),
    all.equal(as.array(dc["green", drop=TRUE]), ar["green",,, drop=TRUE]),
    ## 1x1xN
    # drop=FALSE
    all.equal(dc["green","2015", drop=FALSE], as.data.cube(ar["green","2015",, drop=FALSE])),
    all.equal(as.array(dc["green","2015", drop=FALSE]), ar["green","2015",, drop=FALSE]),
    # drop=TRUE
    all.equal(dc["green","2015", drop=TRUE], as.data.cube(ar["green","2015",, drop=TRUE], dims="status")), # undrop dim name because `drop=T` reduces to 1L vector
    all.equal(as.array(dc["green","2015", drop=TRUE]), ar["green","2015",, drop=TRUE]),
    ## 1x2xN
    # drop=FALSE
    all.equal(dc["green",c("2012","2013"), drop=FALSE], as.data.cube(ar["green",c("2012","2013"),, drop=FALSE])),
    all.equal(as.array(dc["green",c("2012","2013"), drop=FALSE]), ar["green",c("2012","2013"),, drop=FALSE]),
    # drop=TRUE
    all.equal(dc["green",c("2012","2013"), drop=TRUE], as.data.cube(ar["green",c("2012","2013"),, drop=TRUE])),
    all.equal(as.array(dc["green",c("2012","2013"), drop=TRUE]), ar["green",c("2012","2013"),, drop=TRUE]),
    ## 2x2xN
    # drop=FALSE
    all.equal(dc[c("green","red"),c("2012","2013"), drop=FALSE], as.data.cube(ar[c("green","red"),c("2012","2013"),, drop=FALSE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"), drop=FALSE]), ar[c("green","red"),c("2012","2013"),, drop=FALSE]),
    # drop=TRUE
    all.equal(dc[c("green","red"),c("2012","2013"), drop=TRUE], as.data.cube(ar[c("green","red"),c("2012","2013"),, drop=TRUE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"), drop=TRUE]), ar[c("green","red"),c("2012","2013"),, drop=TRUE]),
    ## # 2x2x3
    # drop=FALSE
    all.equal(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE], as.data.cube(ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE]), ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=FALSE]),
    # drop=TRUE
    all.equal(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE], as.data.cube(ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE])),
    all.equal(as.array(dc[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE]), ar[c("green","red"),c("2012","2013"),c("active","archived","inactive"), drop=TRUE]),
    ## # Nx2x3
    # drop=FALSE
    all.equal(dc[,c("2012","2013"),c("active","archived","inactive"), drop=FALSE], as.data.cube(ar[,c("2012","2013"),c("active","archived","inactive"), drop=FALSE])),
    all.equal(as.array(dc[,c("2012","2013"),c("active","archived","inactive"), drop=FALSE]), ar[,c("2012","2013"),c("active","archived","inactive"), drop=FALSE]),
    # drop=TRUE
    all.equal(dc[,c("2012","2013"),c("active","archived","inactive"), drop=TRUE], as.data.cube(ar[,c("2012","2013"),c("active","archived","inactive"), drop=TRUE])),
    all.equal(as.array(dc[,c("2012","2013"),c("active","archived","inactive"), drop=TRUE]), ar[,c("2012","2013"),c("active","archived","inactive"), drop=TRUE])
)

# - [x] dimension is not dropped when filtering for 2+ values (may not be matching at all), even when it evaluates to 1 length in fact
stopifnot(
    all.equal(dim(ar[c("green","red"), as.character(2011:2012), c("inactive","archived")]), dim(dc[c("green","red"), as.character(2011:2012), c("inactive","archived")])),
    all.equal(d <- as.data.cube(ar[c("green","red"), as.character(2011:2012), c("inactive","archived")]), dc[c("green","red"), as.character(2011:2012), c("inactive","archived")]),
    all.equal(format(d)[,lapply(.SD,uniqueN), .SDcols=!"value"], setkey(data.table(color=1L, year=2L, status=2L))),
    # single bad color - not existing in dimension
    all.equal(dim(ar["red", as.character(2011:2012), c("inactive","archived"), drop=FALSE]), dim(dc[c("badcolor","red"), as.character(2011:2012), c("inactive","archived")])),
    # multiple bad colors
    all.equal(dim(ar[NULL, as.character(2011:2012), c("inactive","archived")]), dim(dc[c("badcolor","badcolor2"), as.character(2011:2012), c("inactive","archived")])),
    # query by non existing dim key match to query by NULL
    all.equal(dc["badcolor"], as.data.cube(ar[NULL,,])),
    all.equal(dc["badcolor", drop=FALSE], as.data.cube(ar[NULL,,, drop=FALSE])),
    all.equal(as.array(dc["badcolor"]), ar[NULL,,]),
    all.equal(as.array(dc["badcolor", drop=FALSE]), ar[NULL,,, drop=FALSE])
)

# - [x] slice dims by variables
var.color = c("green","red")
var.status = "archived"
stopifnot(
    all.equal(dc[var.color,,var.status], as.data.cube(ar[var.color,,var.status])),
    all.equal(dc[var.color,,var.status, drop=FALSE], as.data.cube(ar[var.color,,var.status, drop=FALSE]))
)

# - [x] empty calls `dc[]`, `dc[drop=.]`, `dc[,,,]` consistency to array having dim of length 1
ar2 = ar[,"2012",,drop=FALSE]
dc2 = as.data.cube(ar2)
stopifnot(
    all.equal(ar2[], as.array(dc2[])),
    all.equal(ar2[drop=TRUE], as.array(suppressWarnings(dc2[drop=TRUE]))), # warning: drop argument is ignored for calls `dc[drop=.]` for consistency to base::array, for `drop` and empty slices use `dc[, drop=.]`.
    all.equal(ar2[drop=FALSE], as.array(suppressWarnings(dc2[drop=FALSE]))),
    all.equal(ar2[,,], as.array(dc2[,,])),
    all.equal(ar2[,,,drop=TRUE], as.array(dc2[,,,drop=TRUE])),
    all.equal(ar2[,,,drop=FALSE], as.array(dc2[,,,drop=FALSE])),
    # different number of empty args
    all.equal(ar2[,,,drop=TRUE], as.array(dc2[,drop=TRUE])),
    all.equal(ar2[,,,drop=TRUE], as.array(dc2[,,drop=TRUE])),
    all.equal(ar2[,,,drop=TRUE], as.array(dc2[,,,drop=TRUE]))
)

# - [x] NULL subset
stopifnot(
    identical(dim(dc[NULL]), c(0L,5L,4L)),
    identical(dim(dc[NULL,,NULL]), c(0L,5L,0L)),
    identical(dimnames(dc[,NULL,NULL]), list(color = c("green","red","yellow"), year = NULL, status = NULL))
)

# - [x] filter dimensions `.()` ----

# - [x] missing slices equals to `.()`
stopifnot(
    all.equal(dc["green",.(),.()], dc["green"]),
    all.equal(dc["green",.(),.(), drop=FALSE], dc["green", drop=FALSE]),
    all.equal(dc["green",.(),.()], dc[.("green")]),
    all.equal(dc["green",.(),.(), drop=FALSE], dc[.("green"), drop=FALSE]),
    all.equal(dc["green",.(),.()], dc[list("green")]),
    all.equal(dc["green",.(),.(), drop=FALSE], dc[list("green"), drop=FALSE])
)
# - [x] slices matched by dimension names, also partially provided and combined with `.()` will match by position, handling the shift for named dimensions
stopifnot(
    all.equal(dc[status="active",,color="green"], dc["green",,"active"]),
    all.equal(dc[status="active",,color="green", drop=FALSE], dc["green",,"active", drop=FALSE]),
    all.equal(dc[,"active",color=.("green")], dc["green",,"active"]),
    all.equal(dc[,"active",color=.("green"), drop=FALSE], dc["green",,"active", drop=FALSE]),
    all.equal(dc[,"active",color=.(color="green")], dc["green",,"active"]), # second 'color' is field name, here it is equal to dimension name
    all.equal(dc[,"active",color=.(color="green"), drop=FALSE], dc["green",,"active", drop=FALSE]),
    all.equal(dc[status=c("active","archived"), c("green","red"), as.character(2011:2012)], dc[c("green","red"), as.character(2011:2012), c("active","archived")])
)

# - [ ] slice for dimension not present in cube
# dc[color2="green"] # use data.table test function for handling error tests?

# - [x] slice dims by variables
var.color = c("green","red")
var.status = "archived"
stopifnot(
    all.equal(dc[.(var.color),,.(var.status)], as.data.cube(ar[var.color,,var.status])),
    all.equal(dc[.(var.color),,.(var.status), drop=FALSE], as.data.cube(ar[var.color,,var.status, drop=FALSE]))
)

# - [x] NULL subset
stopifnot(
    identical(dim(dc[.(NULL)]), c(0L,5L,4L)),
    identical(dim(dc[,NULL,.(NULL)]), c(3L,0L,0L)),
    identical(dimnames(dc[,.(NULL),.(NULL)]), list(color = c("green","red","yellow"), year = NULL, status = NULL))
)

# - [x] collapse dimensions `+()` ----

stopifnot(
    # collapse dims
    all.equal(apply(ar, 2:3, sum, na.rm=TRUE), as.array(dc[+(.)], na.fill=0)),
    all.equal(apply(ar, 2:3, sum, na.rm=TRUE), as.array(dc[`+`()], na.fill=0)),
    all.equal(apply(ar, 1:2, sum, na.rm=TRUE), as.array(dc[,,+(.)], na.fill=0)),
    all.equal(apply(ar, 1:2, sum, na.rm=TRUE), as.array(dc[,,`+`()], na.fill=0)),
    # collapse dims with filters
    all.equal(apply(ar[c("green","yellow"),,], 2:3, sum, na.rm=TRUE), as.array(dc[+(c("green","yellow"))], na.fill=0)),
    all.equal(apply(ar[c("green","yellow"),,], 2:3, sum, na.rm=TRUE), as.array(dc[`+`(c("green","yellow"))], na.fill=0)),
    # collapse dims with filters multi
    all.equal(apply(ar[, c("2012","2013"), c("active","inactive")], 1L, sum, na.rm=TRUE), as.array(dc[, +(c("2012","2013")), +(c("active","inactive"))], na.fill=0)),
    all.equal(apply(ar[, c("2012","2013"), c("active","inactive")], 1L, sum, na.rm=TRUE), as.array(dc[, `+`(c("2012","2013")), +(c("active","inactive"))], na.fill=0)),
    # collapse one dim with filter, just filter another
    all.equal(apply(ar[, c("2012","2013"), c("active","inactive")], c(1L,3L), sum, na.rm=TRUE), as.array(dc[, +(c("2012","2013")), c("active","inactive")], na.fill=0)),
    all.equal(apply(ar[, c("2012","2013"), c("active","inactive")], c(1L,3L), sum, na.rm=TRUE), as.array(dc[, `+`(c("2012","2013")), .(c("active","inactive"))], na.fill=0))
)

# - [x] collapse dims with filters by variables
var.color = c("green","red")
var.status = c("active","inactive")
stopifnot(
    all.equal(apply(ar[var.color,,var.status], 2L, sum, na.rm=TRUE), as.array(dc[+(var.color),,+(var.status)])),
    all.equal(apply(ar[var.color,,var.status], 1:2, sum, na.rm=TRUE), as.array(dc[.(var.color),,+(var.status)], na.fill=0))
)

# - [x] NULL filter when collapse dim
stopifnot(
    identical(dim(dc[+(NULL)]), c(5L,4L)), # single collapse
    identical(dim(dc[,NULL,+(NULL)]), c(3L,0L)), # partial collapse, partial filter
    identical(dim(dc[,+(NULL),+(NULL)]), 3L) # double collapse
)

# - [ ] rollup dimensions `-()`? ----

## - [ ] multidimensional hierarchical data ----

X = populate_star(1e3)
cb = as.cube(X)
dc = as.data.cube(X)

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
    all.equal(r <- dc[c("Mazda RX4","Honda Civic"),,,.(geog_division_name = c("Mountain","Pacific"))]$setindex(drop=TRUE), 
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
stopifnot(all.equal( # reorder with shift matching by position
    dc["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))],
    dc[time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2")),
       currency = .(curr_type = "crypto"),
       "Mazda RX4", # product
       .(), # customer
       geography = .()]
))
stopifnot( # NULL subset
    identical(dim(dc[NULL]), c(0L,32L,49L,50L,1826L)),
    identical(dim(dc[.(NULL)]), c(0L,32L,49L,50L,1826L)),
    identical(dim(dc[,,.(NULL),,NULL]), c(32L,32L,0L,50L,0L)),
    identical(dim(dc[,,,,.(time_year = 2014L, time_quarter_name = NULL)]), c(32L,32L,49L,50L,0L))
)
stopifnot( # drop arg
    length(dim(dc[]))==5L,
    length(dim(dc["Mazda RX4"]))==4L,
    length(dim(dc["Mazda RX4", drop=FALSE]))==5L
)
