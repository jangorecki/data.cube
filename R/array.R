as.data.table.array = function(x, na.rm=TRUE){
    dimnms = names(dimnames(x))
    if("value" %in% dimnms) stop("Array to convert cannot have `value` dimension name as the name will be used for a measure, rename dimname of array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dimnms), as.symbol)))),, keyby = c(dimnms)]
    if(na.rm) r = r[!is.na(value)]
    r
}

estimate_bytes = function(type, len){
    switch(type,
           "integer" = 4,
           "double" = 8,
           "character" = 56,
           "logical" = 4,
           "complex" = 16) * len
}

as.array.cube = function(x, measure, force=FALSE){
    if(missing(measure)) measure = x$measures[1L]
    if(!force){
        len = prod(x$nr[x$dims])
        if(identical(.Platform$OS.type, "unix")){
            free_bytes = as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE))
        } else {
            # PR welcome for windows, mac, etc.
            free_bytes = NA_real_
        }
        estimated_bytes = estimate_bytes(typeof(x$db[[x$fact]][[measure]]), len)
        if(is.na(free_bytes)){
            warning("Unable to check free memory before conversion `cube` to `array`.")
        }
        if(estimated_bytes > free_bytes){
            stop("Aborting conversion `cube` to `array` due to not enough memory. If you really want to try hang your R session call `as.array` with `force=TRUE` argument.")
        }
    }
    # - [x] proceed when enough free memory or force=TRUE
    dimnms = lapply(setNames(x$dims, x$dims), function(dim) as.character(x$db[[dim]][[1L]]))
    array(data = x$db[[x$fact]][do.call(CJ, dimnms), eval(as.name(measure))],
          dim = sapply(dimnms, length),
          dimnames = dimnms)
    
}