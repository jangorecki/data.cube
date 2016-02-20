library(data.table)
library(data.cube)

### no hierarchy ---------------------------------------------------------

## cube in-out ----------------------------------------------------------------------

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")), 
                   year = as.character(2011:2015), 
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)
dt = as.data.table(ar)
l = list(fact = list(fact=dt), dims = mapply(process.dim, names(ar.dimnames), ar.dimnames, SIMPLIFY = FALSE))
# str(l, 2L, give.attr = FALSE)

# in
cb.ar = as.cube(ar)
cb.dt = as.cube(dt, dims = sapply(names(ar.dimnames), function(x) x, simplify = FALSE))
cb.l = as.cube(l)
stopifnot(all.equal(cb.ar, cb.dt), all.equal(cb.ar, cb.l))

# out
stopifnot(
    all.equal(l, as.list(cb.l)), 
    all.equal(dt, as.data.table(cb.dt)), 
    all.equal(ar, as.array(cb.ar))
)

# zero dimension fact
r = aggregate(cb.l, character(), FUN = sum)
stopifnot(
    nrow(format(r))==1L,
    all.equal(format(r), as.data.table(r)),
    all.equal(format(r)$value, as.array(r))
)

### hierarchy ---------------------------------------------------------------

## cube in-out --------------------------------------------------------------

l = populate_star(1e3)

# in
cb.l = as.cube(l)
dt = cb.l$denormalize()
cb.dt = as.cube(dt, fact = "sales", dims = lapply(l$dims, names))
stopifnot(
    is.cube(cb.dt),
    # all.equal(cb.l, cb.dt) # cannot be equal because cb.dt has already dropped unused dimension values, so there is a different subset of date
    all.equal(cb.l$env$fact, cb.dt$env$fact),
    all.equal(cb.l$dims, cb.dt$dims)
)

# out
stopifnot(all.equal(l, as.list(cb.l, fact = "sales")), all.equal(dt, as.data.table(cb.dt)))

# tests status ------------------------------------------------------------

invisible(TRUE)
