as.data.table.array = function(x, na.rm=TRUE){
    dimnms = names(dimnames(x))
    if("value" %in% dimnms) stop("Array to convert must not already have `value` character as dimension name. `value` name is reserved to converted measure, rename dimname of input array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dimnms), as.symbol)))),, keyby = c(dimnms)]
    if(na.rm) r = r[!is.na(value)]
    r
}

as.array.data.table = function(x, dimnames, measure){
    stopifnot(is.list(dimnames), !is.null(names(dimnames)), length(names(dimnames))==length(unique(names(dimnames))), all(sapply(dimnames, is.character)), all(sapply(dimnames, length)), is.character(measure))
    revkey = rev(names(dimnames))
    crossdims = quote(setkeyv(do.call(CJ, dimnames), revkey))
    array(data = x[eval(crossdims), eval(as.name(measure)), on = c(revkey)],
          dim = sapply(dimnames, length),
          dimnames = dimnames)
}
