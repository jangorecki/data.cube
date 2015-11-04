library(data.table)
library(data.cube)
set.seed(1L)

### no hierarchy ---------------------------------------------------------

## array-data.table -------------------------------------------------------

dimnames = list(color = sort(c("green","yellow","red")), 
                year = as.character(2011:2015), 
                status = sort(c("active","inactive","archived","removed")))
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(sapply(dimnames, length)), TRUE), 
           unname(sapply(dimnames, length)), 
           dimnames)
dt = as.data.table(ar)
ar2 = as.array(dt, dimnames)
stopifnot(all.equal(ar, ar2))

## cube in-out ----------------------------------------------------------------------

l = list(fact = list(fact=dt), dims = mapply(setnames, lapply(dimnames, as.data.table), names(dimnames), SIMPLIFY = FALSE))
 
# in
cb = as.cube(ar)
cb2 = as.cube(dt, dims = sapply(names(dimnames), function(x) x, simplify = FALSE))
cb3 = as.cube(l)
stopifnot(all.equal(cb, cb2), all.equal(cb, cb3))

# out
l2 = as.list(cb)
dt2 = as.data.table(cb)
ar2 = as.array(cb)
stopifnot(all.equal(l, l2), all.equal(dt, dt2), all.equal(ar, ar2))

## cube subset method --------------------------------------------------------------

# subset NULL/missing handling --------------------------------------------

stopifnot(
    # keep all hierachies
    all.equal(cb[,,], cb[.(),.(),.()])
    # filter by atomic type
    , all.equal(cb["green",], cb[.(color = "green"),.()])
    # NULL cube subset
    , all.equal(cb[NULL,,NULL], cb[NULL,.(),NULL])
    # wraps in a list atomic types cb[.(),.(),.(curr_name = c("CAD","EUR")),.()]
    , all.equal(cb[c("green","red"),], cb[.(color = c("green","red")), .()])
    # drop all attributes of hierarchy, keeps key
    , all.equal(cb[.(NULL),], cb[.(color=NULL),.()])
)

# subset array way + drop argument ----------------------------------------------

stopifnot(
    # 1xNxN
    all.equal(as.array(cb["green",drop=FALSE]), ar["green",,,drop=FALSE])
    , all.equal(as.array(cb["green",drop=TRUE]), ar["green",,,drop=TRUE])
    # 2xNxN
    , all.equal(as.array(cb[c("green","red"),drop=FALSE]), ar[c("green","red"),,,drop=FALSE])
    , all.equal(as.array(cb[c("green","red"),drop=TRUE]), ar[c("green","red"),,,drop=TRUE])
    # 1x2xN
    , all.equal(as.array(cb["green",c("2011","2012"),drop=FALSE]), ar["green",c("2011","2012"),,drop=FALSE])
    , all.equal(as.array(cb["green",c("2011","2012"),drop=TRUE]), ar["green",c("2011","2012"),,drop=TRUE])
    # 1x2x3
    , all.equal(as.array(cb["green",c("2011","2012"),"active",drop=FALSE]), ar["green",c("2011","2012"),"active",drop=FALSE])
    , all.equal(as.array(cb["green",c("2011","2012"),"active",drop=TRUE]), ar["green",c("2011","2012"),"active",drop=TRUE])
    # 1x1x1
    , all.equal(as.array(cb["green","2012","active",drop=FALSE]), ar["green","2012","active",drop=FALSE])
    , all.equal(as.array(cb["green","2012","active",drop=TRUE]), ar["green","2012","active",drop=TRUE])
    , all.equal(as.array(cb["green","2014","active",drop=FALSE]), ar["green","2014","active",drop=FALSE])
    , all.equal(as.array(cb["green","2014","active",drop=TRUE]), ar["green","2014","active",drop=TRUE])
    # 2x2x1
    , all.equal(as.array(cb[c("green","red"),c("2011","2012"),"active",drop=FALSE]), ar[c("green","red"),c("2011","2012"),"active",drop=FALSE])
    , all.equal(as.array(cb[c("green","red"),c("2011","2012"),"active",drop=TRUE]), ar[c("green","red"),c("2011","2012"),"active",drop=TRUE])
    # 2xNx2
    , all.equal(as.array(cb[c("green","red"),,c("active","inactive"),drop=FALSE]), ar[c("green","red"),,c("active","inactive"),drop=FALSE])
    , all.equal(as.array(cb[c("green","red"),,c("active","inactive"),drop=TRUE]), ar[c("green","red"),,c("active","inactive"),drop=TRUE])
    # 1xNx1
    , all.equal(as.array(cb["green",,"active",drop=FALSE]), ar["green",,"active",drop=FALSE])
    , all.equal(as.array(cb["green",,"active",drop=TRUE]), ar["green",,"active",drop=TRUE])
    # NxNx1
    , all.equal(as.array(cb[,,"active",drop=FALSE]), ar[,,"active",drop=FALSE])
    , all.equal(as.array(cb[,,"active",drop=TRUE]), ar[,,"active",drop=TRUE])
    # NxNx2
    , all.equal(as.array(cb[,,c("active","inactive"),drop=FALSE]), ar[,,c("active","inactive"),drop=FALSE])
    , all.equal(as.array(cb[,,c("active","inactive"),drop=TRUE]), ar[,,c("active","inactive"),drop=TRUE])
    # Nx1x1
    , all.equal(as.array(cb[,"2012","active",drop=FALSE]), ar[,"2012","active",drop=FALSE])
    , all.equal(as.array(cb[,"2012","active",drop=TRUE]), ar[,"2012","active",drop=TRUE])
    # Nx2x2
    , all.equal(as.array(cb[,c("2011","2012"),c("active","inactive"),drop=FALSE]), ar[,c("2011","2012"),c("active","inactive"),drop=FALSE])
    , all.equal(as.array(cb[,c("2011","2012"),c("active","inactive"),drop=TRUE]), ar[,c("2011","2012"),c("active","inactive"),drop=TRUE])
    # 0x0x0
    , all.equal(as.array(cb[NULL,NULL,NULL,drop=FALSE]), ar[NULL,NULL,NULL,drop=FALSE])
    , all.equal(as.array(cb[NULL,NULL,NULL,drop=TRUE]), ar[NULL,NULL,NULL,drop=TRUE])
    # 0xNxN
    , all.equal(as.array(cb[NULL,,,drop=FALSE]), ar[NULL,,,drop=FALSE])
    , all.equal(as.array(cb[NULL,,,drop=TRUE]), ar[NULL,,,drop=TRUE])
    # Nx2x0
    , all.equal(as.array(cb[,c("2011","2012"),NULL,drop=FALSE]), ar[,c("2011","2012"),NULL,drop=FALSE])
    , all.equal(as.array(cb[,c("2011","2012"),NULL,drop=TRUE]), ar[,c("2011","2012"),NULL,drop=TRUE])
    # 1x1x0
    , all.equal(as.array(cb["green","2012",NULL,drop=FALSE]), ar["green","2012",NULL,drop=FALSE])
    , all.equal(as.array(cb["green","2012",NULL,drop=TRUE]), ar["green","2012",NULL,drop=TRUE])
    # 1xNx0
    , all.equal(as.array(cb["green",,NULL,drop=FALSE]), ar["green",,NULL,drop=FALSE])
    , all.equal(as.array(cb["green",,NULL,drop=TRUE]), ar["green",,NULL,drop=TRUE])
)

# subset using list to refer hierarchies

# lookup columns without subset

# lookup whole dimension

# cube apply method

### hierarchy ---------------------------------------------------------------

X = populate_star(1e5)
invisible()

# # some NULL/missing handling
# , all.equal(cb[,,.()], cb[.(),.(),.(curr_name = NULL, curr_type = NULL)]) # expand for all dim attributes 

