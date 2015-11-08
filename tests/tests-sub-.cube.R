library(data.table)
library(data.cube)

### no hierarchy ----------------------------------------------------------

# subset NULL/missing handling --------------------------------------------

set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")), 
                   year = as.character(2011:2015), 
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)
cb = as.cube(ar)

stopifnot(
    # keep all hierachies
    all.equal(cb[,,], cb[.(),.(),.()])
    # filter by atomic type
    , all.equal(cb["green",], cb[.(color = "green"),.()])
    # NULL cube subset
    , all.equal(cb[NULL,,NULL], cb[NULL,.(),NULL])
    # wraps in a list atomic types cb[.(),.(),.(curr_name = c("CAD","EUR")),.()]
    , all.equal(cb[c("green","red"),], cb[.(color = c("green","red")), .()])
    # NULL subset
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

### hierarchy ---------------------------------------------------------------

# slice and dice on dimension hierarchy -----------------------------------

# cb["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# cb$dims
# cb[product = "Mazda RX4",
#    customer = NULL,
#    currency = .(curr_type = "crypto"),
#    geography = NULL,
#    time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
