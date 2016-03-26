as.array.data.cube = function(x, measure, na.fill = NA, ...) {
    if (missing(measure)) measure = x$fact$measure.vars[1L]
    if (length(measure) > 1L) stop("Your cube seems to have multiple measures, you must provide scalar column name as 'measure' argument to as.array.")
    dimcols = lapply(x$dimensions, function(x) x$id.vars)
    stopifnot(sapply(dimcols, length) == 1L) # every key should be a single column key
    r = as.array(x = x$fact$data, dimcols = unlist(dimcols), measure = measure, dimnames = dimnames(x))
    # below will be removed after data.table#857 resolved, logic will goes into cb$denormalize method
    if (!is.na(na.fill)) r[is.na(r)] = na.fill
    r
}

as.data.table.level = function(x) {
    x$data
}

as.data.table.dimension = function(x, lvls = names(x$levels)) {
    stopifnot(is.dimension(x))
    r = copy(x$data)
    lookupv(dims = lapply(x$levels[lvls], as.data.table.level), r)
    r
}

as.data.table.data.cube = function(x, na.fill = FALSE, dcast = FALSE, ...) {
    r = x$denormalize(na.fill = na.fill)
    if (isTRUE(dcast)) dcast.data.table(r, ...) else r
}

lookupv = function(dims, fact) {
    if (!length(dims)) return(logical(0))
    sapply(dims, function(dim) {
        nd = copy(names(dim))
        nf = copy(names(fact))
        lookup(fact, dim, setdiff(nd, nf))
    })
}
