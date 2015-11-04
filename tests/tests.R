library(data.table)
library(data.cube)
set.seed(1L)

### no hierarchy ---------------------------------------------------------

# array-data.table -------------------------------------------------------

dimnames = list(color = sort(c("green","yellow","red")), 
                year = as.character(2011:2015), 
                status = sort(c("active","inactive","archived","removed")))
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(sapply(dimnames, length)), TRUE), 
           unname(sapply(dimnames, length)), 
           dimnames)
dt = as.data.table(ar)
ar2 = as.array(dt, dimnames)
stopifnot(all.equal(ar, ar2))

# cube in-out ----------------------------------------------------------------------

l = list(fact = list(fact=dt), dims = mapply(setnames, lapply(dimnames, as.data.table), names(dimnames), SIMPLIFY = FALSE))
 
# in
cb = as.cube(ar)
cb2 = as.cube(dt, dims = sapply(names(dimnames), function(x) x, simplify = FALSE))
cb3 = as.cube(l)
stopifnot(all.equal(cb, cb2), all.equal(cb, cb3))

# out
l2 = as.list(cb)
# dt2 = as.data.table(cb)
ar2 = as.array(cb)
stopifnot(all.equal(l, l2), TRUE, all.equal(ar, ar2))
# all.equal(dt, dt2)

# cube subset method --------------------------------------------------------------

# some NULL/missing handling
stopifnot(
    all.equal(cb[,,], cb[.(),.(),.()]) # keep all hierachies
    , all.equal(cb["green",], cb[.(color = "green"),.()]) # filter by atomic type
    , all.equal(cb[NULL,,NULL], cb[NULL,.(),NULL]) # NULL cube subset
    , all.equal(cb[c("green","red"),], cb[.(color = c("green","red")), .()]) # wraps in a list atomic types cb[.(),.(),.(curr_name = c("CAD","EUR")),.()]
    , all.equal(cb[.(NULL),], cb[.(color=NULL),.()]) # drop all attributes of hierarchy, keeps key
)

# subset array way

# subset drop argument
stopifnot(
    all.equal(as.array(cb["green",drop=FALSE]), ar["green",,,drop=FALSE])
)

# remove drop of dim keys?
# as.data.table(as.array(cb["green",drop=FALSE]))
# ar["green",,,drop=TRUE]

# subset using list to refer hierarchies

# lookup columns without subset

# lookup whole dimension

# cube apply method

# hierarchy ---------------------------------------------------------------

X = populate_star(1e5)
invisible()

# # some NULL/missing handling
# , all.equal(cb[,,.()], cb[.(),.(),.(curr_name = NULL, curr_type = NULL)]) # expand for all dim attributes 

