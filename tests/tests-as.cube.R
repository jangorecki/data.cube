library(data.table)
library(data.cube)
set.seed(1L)

### no hierarchy ---------------------------------------------------------

## array-data.table -------------------------------------------------------

ar.dimnames = list(color = sort(c("green","yellow","red")), 
                   year = as.character(2011:2015), 
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)
dt = as.data.table(ar)
stopifnot(all.equal(ar, as.array(dt, ar.dimnames)))

## cube in-out ----------------------------------------------------------------------

l = list(fact = list(fact=dt), dims = mapply(process.dim, names(ar.dimnames), ar.dimnames, SIMPLIFY = FALSE))
# str(l, 2L, give.attr = FALSE)

# in
cb = as.cube(ar)
cb2 = as.cube(dt, dims = sapply(names(ar.dimnames), function(x) x, simplify = FALSE))
cb3 = as.cube(l)
stopifnot(all.equal(cb, cb2), all.equal(cb, cb3))

# out
stopifnot(all.equal(l, as.list(cb)), all.equal(dt, as.data.table(cb)), all.equal(ar, as.array(cb)))

### hierarchy ---------------------------------------------------------------

## cube in-out --------------------------------------------------------------

# set.seed(1L)
# X = populate_star(1e5)
# cb = as.cube(X)
# l = 
#     dt = as.data.table(cb)
# 
# stopifnot(TRUE)
# all.equal(X, as.list(cb, fact="sales"))
# all.equal(cb, as.cube(dt, x))
