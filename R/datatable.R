as.data.table.array = function(x, na.rm=TRUE){
    dimnms = names(dimnames(x))
    if("value" %in% dimnms) stop("Array to convert must not already have `value` character as dimension name. `value` name is reserved to converted measure, rename dimname of input array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dimnms), as.symbol)))),, keyby = c(dimnms)]
    if(na.rm) r = r[!is.na(value)]
    r
}

as.array.data.table = function(x, dimnames, measure, ...){
    stopifnot(is.list(dimnames), !is.null(names(dimnames)), length(names(dimnames))==length(unique(names(dimnames))), all(sapply(dimnames, is.character)), all(sapply(dimnames, length)))
    revkey = rev(names(dimnames))
    if(missing(measure)){
        if(length(x)==length(revkey)+1L) measure = names(x)[length(x)] # if only one measure then use as default for `measure` arg
    }
    stopifnot(is.character(measure), length(measure)==1L)
    crossdims = quote(setkeyv(do.call(CJ, dimnames), revkey))
    array(data = x[eval(crossdims), eval(as.name(measure)), on = c(revkey)],
          dim = sapply(dimnames, length),
          dimnames = dimnames)
}

is.unique.data.table = function(x, by = key(x)){
    nrow(x)==nrow(unique(x, by = by))
}
