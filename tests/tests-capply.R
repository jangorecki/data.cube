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

### hierarchy ---------------------------------------------------------------

# X = populate_star(1e5)
# cb = as.cube(X) # na.rm=TRUE
