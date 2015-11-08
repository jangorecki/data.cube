as.data.table.array = function(x, na.rm=TRUE, ...){
    dimnms = names(dimnames(x))
    if("value" %in% dimnms) stop("Array to convert must not already have `value` character as dimension name. `value` name is reserved to converted measure, rename dimname of input array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dimnms), as.symbol)))),, keyby = c(dimnms)]
    if(na.rm) r[!is.na(value)] else r
}

as.array.data.table = function(x, dimnames, measure, ...){
    stopifnot(is.list(dimnames), !is.null(names(dimnames)), length(names(dimnames))==length(unique(names(dimnames))))
    revkey = rev(names(dimnames))
    if(missing(measure)){
        if(length(x)==length(revkey)+1L) measure = names(x)[length(x)] # if only one measure then use it as default for `measure` arg
    }
    stopifnot(is.character(measure), length(measure)==1L)
    if(!length(dimnames)) return(x[, eval(as.name(measure))])
    if(nrow(x)){
        crossdims = quote(setkeyv(do.call(CJ, dimnames), revkey))
        # check if `on` cols exists to avoid 1.9.6 Error in forderv - fixed in 1.9.7 already - data.table#1376
        if(!all(revkey %in% names(x))) stop(sprintf("Columns to join on does not exists in data.table '%s'.", paste(revkey[!revkey %in% names(x)], collapse=", ")))
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
