as.data.table.array = function(x, na.rm=TRUE){
    dimnms = names(dimnames(x))
    if("value" %in% dimnms) stop("Array to convert cannot have `value` dimension name as the name will be used for a measure, rename dimname of array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dimnms), as.symbol)))),, keyby = c(dimnms)]
    if(na.rm) r = r[!is.na(value)]
    r
}

as.array.cube = function(x, measure){
    if(missing(measure)) measure = x$measures[1L]
    len = prod(x$nr[x$dims])
    if(identical(.Platform$OS.type, "unix")){
        freebytes = as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE))
    } else {
        # PR welcome for windows, mac, etc.
        freebytes = NA_real_
    }
    browser()
}