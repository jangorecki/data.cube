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

## na.rm=FALSE - all dims cross product will not scale, read *curse of dimensionality*

cb = as.cube(ar, na.rm = FALSE)

stopifnot(
    # two dims sum, na.rm=FALSE
    all.equal(
        as.array(capply(cb, c("year","status"), sum, na.rm=FALSE)),
        apply(ar, c("year","status"), sum, na.rm=FALSE)
    ),
    # two dims mean, na.rm=FALSE
    all.equal(
        as.array(capply(cb, c("year","status"), mean, na.rm=FALSE)),
        apply(ar, c("year","status"), mean, na.rm=FALSE)
    ),
    # lack of na.rm arg
    all.equal(
        as.array(capply(cb, c("year","status"), sum)),
        apply(ar, c("year","status"), sum)
    )
)

## na.rm=TRUE - scales well for cube class

cb = as.cube(ar)

stopifnot(
    # two dims sum, na.rm=TRUE
    all.equal(
        as.array(capply(cb, c("year","status"), sum, na.rm=TRUE), na.fill = 0), # sum's results NA are 0 for array
        apply(ar, c("year","status"), sum, na.rm=TRUE)
    ),
    # two dims mean, na.rm=TRUE
    all.equal(
        as.array(capply(cb, c("year","status"), mean, na.rm=TRUE), na.fill = NaN), # mean's results NA are NaN for array
        apply(ar, c("year","status"), mean, na.rm=TRUE)
    ),
    # single dimension
    all.equal(
        as.array(capply(cb, "year", sum, na.rm=TRUE), na.fill = 0),
        apply(ar, "year", sum, na.rm=TRUE)
    ),
    # single dimension value, drop FALSE
    all.equal(
        as.array(capply(cb[,"2014",,drop=FALSE], "year", sum, na.rm=TRUE), na.fill = 0),
        apply(ar[,"2014",,drop=FALSE], "year", sum, na.rm=TRUE)
    ),
    # limited dimension value
    all.equal(
        as.array(capply(cb[,c("2014","2015"),], "year", sum, na.rm=TRUE), na.fill = 0),
        apply(ar[,c("2014","2015"),], "year", sum, na.rm=TRUE)
    ),
    # all dimensions
    all.equal(
        as.array(capply(cb, c("color","year","status"), sum, na.rm=TRUE), na.fill = 0),
        apply(ar, c("color","year","status"), sum, na.rm=TRUE)
    )
)

# aggregate.cube
by = c("year","status")
stopifnot(
    all.equal(aggregate(cb, by, sum, na.rm=FALSE), capply(cb, by, sum, na.rm=FALSE)),
    all.equal(aggregate(cb, by, sum, na.rm=TRUE), capply(cb, by, sum, na.rm=TRUE)),
    all.equal(aggregate(cb, by, sum), capply(cb, by, sum))
)

# grand total aggregation
r = aggregate(cb, character(), sum)
stopifnot(
    is.null(dim(r)),
    is.null(dimnames(r)),
    all.equal(r, capply(cb, character(), sum)),
    nrow(as.data.table(r))==1L
)

### hierarchy ---------------------------------------------------------------

cb = as.cube(populate_star(1e5)) # na.rm=TRUE

# by 1 low attribute from 1 dimension
r = capply(cb, "geog_abb", sum)
stopifnot(
    all.equal(dim(r), 50L),
    all.equal(names(r$env$fact$sales), c("geog_abb","amount","value")),
    all.equal(names(r$env$dims), "geography"),
    all.equal(names(r$env$dims$geography), "geog_abb")
)

# by 1 high attribute from 1 dimension
r = capply(cb, "geog_division_name", sum)
stopifnot(
    all.equal(dim(r), 9L),
    all.equal(names(r$env$fact$sales), c("geog_division_name","amount","value")),
    all.equal(names(r$env$dims), "geography"),
    all.equal(names(r$env$dims$geography), "geog_division_name")
)

# by 2 low attributes from 2 dimensions
r = capply(cb, c("time_date","geog_abb"), sum)
stopifnot(
    all.equal(dim(r), c(1826L, 50L)),
    all.equal(names(r$env$fact$sales), c("time_date","geog_abb","amount","value")),
    all.equal(names(r$env$dims), c("time","geography")),
    all.equal(names(r$env$dims$time), "time_date"),
    all.equal(names(r$env$dims$geography), "geog_abb")
)

# by 2 high attributes from 2 dimensions
r = capply(cb, c("time_year","geog_division_name"), sum)
stopifnot(
    all.equal(dim(r), c(5L, 9L)),
    all.equal(names(r$env$fact$sales), c("time_year","geog_division_name","amount","value")),
    all.equal(names(r$env$dims), c("time","geography")),
    all.equal(names(r$env$dims$time), "time_year"),
    all.equal(names(r$env$dims$geography), "geog_division_name")
)

# by 1 low and 1 high attributes from 2 dimensions
r = capply(cb, c("time_date","geog_division_name"), sum)
stopifnot(
    all.equal(dim(r), c(1826L, 9L)),
    all.equal(names(r$env$fact$sales), c("time_date","geog_division_name","amount","value")),
    all.equal(names(r$env$dims), c("time","geography")),
    all.equal(names(r$env$dims$time), "time_date"),
    all.equal(names(r$env$dims$geography), "geog_division_name")
)

# by 3 low attributes from 3 dimensions
r = capply(cb, c("time_date","geog_abb","curr_name"), sum)
stopifnot(
    all.equal(dim(r), c(1826L, 50L, 49L)),
    all.equal(names(r$env$fact$sales), c("time_date","geog_abb","curr_name","amount","value")),
    all.equal(names(r$env$dims), c("time","geography","currency")),
    all.equal(names(r$env$dims$time), "time_date"),
    all.equal(names(r$env$dims$geography), "geog_abb"),
    all.equal(names(r$env$dims$currency), "curr_name")
)

# by 3 high attributes from 3 dimensions
r = capply(cb, c("time_year","geog_division_name","curr_type"), sum)
stopifnot(
    all.equal(dim(r), c(5L, 9L, 2L)),
    all.equal(names(r$env$fact$sales), c("time_year","geog_division_name","curr_type","amount","value")),
    all.equal(names(r$env$dims), c("time","geography","currency")),
    all.equal(names(r$env$dims$time), "time_year"),
    all.equal(names(r$env$dims$geography), "geog_division_name"),
    all.equal(names(r$env$dims$currency), "curr_type")
)

# by 2 high and 2 low attributes from 4 dimensions
r = capply(cb, c("time_year","prod_name","geog_division_name","curr_name"), sum)
stopifnot(
    all.equal(dim(r), c(5L, 32L, 9L, 49L)),
    all.equal(names(r$env$fact$sales), c("time_year","prod_name","geog_division_name","curr_name","amount","value")),
    all.equal(names(r$env$dims), c("time","product","geography","currency")),
    all.equal(names(r$env$dims$time), "time_year"),
    all.equal(names(r$env$dims$product), "prod_name"),
    all.equal(names(r$env$dims$geography), "geog_division_name"),
    all.equal(names(r$env$dims$currency), "curr_name")
)

# by 1 low and 1 high from same dimension
r = capply(cb, c("geog_abb","geog_division_name"), sum)
stopifnot(
    all.equal(dim(r), 50L),
    all.equal(names(r$env$fact$sales), c("geog_abb","geog_division_name","amount","value")),
    all.equal(names(r$env$dims), "geography"),
    all.equal(names(r$env$dims$geography), c("geog_abb","geog_division_name"))
)

# aggregate.cube
by = c("time_year","prod_name","geog_division_name","curr_name")
stopifnot(
    all.equal(aggregate(cb, by, sum, na.rm=FALSE), capply(cb, by, sum, na.rm=FALSE)),
    all.equal(aggregate(cb, by, sum, na.rm=TRUE), capply(cb, by, sum, na.rm=TRUE)),
    all.equal(aggregate(cb, by, sum), capply(cb, by, sum))
)

# tests status ------------------------------------------------------------

invisible(TRUE)
