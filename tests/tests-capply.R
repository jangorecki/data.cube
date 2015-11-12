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

# stopifnot(
#     # two dims, na.rm=TRUE
#     all.equal(
#         as.array(capply(cb, c("year","status"), sum, na.rm=TRUE)),
#         apply(ar, c("year","status"), sum, na.rm=TRUE)
#     ),
#     # two dims, na.rm=FALSE
#     all.equal(
#         as.array(capply(cb, c("year","status"), sum, na.rm=FALSE)),
#         apply(ar, c("year","status"), sum, na.rm=FALSE)
#     ),
#     # single dimension
#     all.equal(
#         as.array(capply(cb, "year", sum, na.rm=TRUE)),
#         apply(ar, "year", sum, na.rm=TRUE)
#     ),
#     # single dimension value, drop FALSE
#     all.equal(
#         as.array(capply(cb[,"2014",,drop=FALSE], "year", sum, na.rm=TRUE)),
#         apply(ar[,"2014",,drop=FALSE], "year", sum, na.rm=TRUE)
#     ),
#     # all dimensions
#     all.equal(
#         as.array(capply(cb, c("color","year","status"), sum, na.rm=TRUE)),
#         apply(ar, c("color","year","status"), sum, na.rm=TRUE)
#     )
# )

### hierarchy ---------------------------------------------------------------

# X = populate_star(1e5)
# cb = as.cube(X)
