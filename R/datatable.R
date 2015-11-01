as.data.table.array = function(x, na.rm=TRUE){
    dimnms = names(dimnames(x))
    if("value" %in% dimnms) stop("Array to convert must not already have `value` character as dimension name. `value` name is reserved to converted measure, rename dimname of input array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dimnms), as.symbol)))),, keyby = c(dimnms)]
    if(na.rm) r = r[!is.na(value)]
    r
}
