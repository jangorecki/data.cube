as.data.table.array = function(x, na.rm=TRUE){
    dimnms = names(dimnames(x))
    if("value" %in% dimnms) stop("Array to convert must not already have `value` character as dimension name. `value` name is reserved to converted measure, rename dimname of input array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dimnms), as.symbol)))),, keyby = c(dimnms)]
    if(na.rm) r = r[!is.na(value)]
    r
}

as.array.data.table = function(x, dimnames, measure, ...){
    stopifnot(is.list(dimnames), !is.null(names(dimnames)), length(names(dimnames))==length(unique(names(dimnames))), all(sapply(dimnames, is.character)))
    revkey = rev(names(dimnames))
    if(missing(measure)){
        if(length(x)==length(revkey)+1L) measure = names(x)[length(x)] # if only one measure then use it as default for `measure` arg
    }
    stopifnot(is.character(measure), length(measure)==1L)
    if(!length(dimnames)) return(x[, eval(as.name(measure))])
    if(nrow(x)){
        crossdims = quote(setkeyv(do.call(CJ, dimnames), revkey))
        r = array(data = x[eval(crossdims), eval(as.name(measure)), on = c(revkey)],
                  dim = unname(sapply(dimnames, length)),
                  dimnames = dimnames)
    } else {
        r = array(data = x[, eval(as.name(measure))],
                  dim = unname(sapply(dimnames, length)),
                  dimnames = dimnames)
    }
    if(length(dimnames)==1L) c(r) else r
}

is.unique.data.table = function(x, by = key(x)){
    nrow(x)==nrow(unique(x, by = by))
}
