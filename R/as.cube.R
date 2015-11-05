# as.cube -----------------------------------------------------------------

#' @title Cast to OLAP cube
#' @param x R object
#' @param \dots arguments passed to methods
#' @description Converts arguments to *cube* class. Supports *list*, *array* (no hierarchies), *data.table*.
#' @return *cube* class object.
as.cube = function(x, ...){
    UseMethod("as.cube")
}

as.cube.default = function(x, ...){
    as.cube(as.array(x, ...))
}

as.cube.array = function(x, fact = "fact", na.rm=TRUE, ...){
    stopifnot(is.character(fact), length(fact)==1L)
    dims = selfNames(names(dimnames(x)))
    as.cube(as.data.table(x, na.rm = na.rm), fact = fact, dims = lapply(selfNames(names(dimnames(x))), function(x) x))
}

#' @title Process dimension
#' @param dim character scalar name of dimension
#' @param x list value for element *dim*
#' @description Helps in processing dimension data for results of *dimnames.array* function. It also check *key* and it's uniqueness.
process_dim = function(dim, x){
    if(!is.data.table(x)){
        if(is.atomic(x)) x = setDT(setNames(list(unique(x)), dim))
        else if(is.data.frame(x)) setDT(x)
        else stop("Unsupported type of dimension values.")
    }
    if(!identical(key(x), names(x)[1L])) setkeyv(x, names(x)[1L])
    if(!is.unique.data.table(x)){
        nr_before = nrow(x)
        x = unique(x, by = key(x))
        nr_after = nrow(x)
        warning(sprintf("'%s' dimension hierarchy is broken, table key is not unique. Some entries in dimension hierarchy has been dropped to enforce unique key on %s. Input rows %s while cardinality is %s.", dim, key(x), nr_before, nr_after))
    }
    x
}

# @param dims list of vectors columns names for each dimension
as.cube.data.table = function(x, fact = "fact", dims, fun.aggregate = sum, ...){
    stopifnot(is.data.table(x), is.character(fact), is.list(dims), as.logical(length(dims)), length(names(dims))==length(unique(names(dims))), all(sapply(dims, is.character)), all(sapply(dims, length)), is.function(fun.aggregate))
    key_cols = sapply(dims, `[`, 1L)
    measure_cols = names(x)[!names(x) %in% unlist(dims)]
    cube$new(list(
        fact = setNames(list(x[, lapply(.SD, fun.aggregate, ...), c(key_cols), .SDcols = measure_cols]), fact), 
        dims = lapply(selfNames(names(dims)), function(dim) process_dim(dim, x = x[[dim]]))
    ))
}

# @param dist list of data.table dimension tables
as.cube.list = function(x, fact, dims, fun.aggregate = sum, ...){
    if(!missing(fact) & !missing(dims)){
        stopifnot(is.list(fact), length(fact)==1L, is.data.table(fact[[1L]]), is.list(dims))
        x = list(fact = fact, dims = dims)
    }
    stopifnot(is.list(x), all(c("fact","dims") %in% names(x)))
    # - [ ] decode base R *dimnames* structure `list(dim1=c(...),dim2=c(...))` to support it as an input
    x$dims = lapply(selfNames(names(x$dims)), function(dim) process_dim(dim, x = x$dims[[dim]]))
    fact = names(x$fact)
    if(!is.unique.data.table(x$fact[[fact]])){
        if(missing(fun.aggregate)) stop(sprintf("Fact table is not sub-aggregated and the `fun.aggregate` argument is missing. Sub-aggregated your fact table or provide aggregate function to be used on all measures."))
        if(!is.function(fun.aggregate)) stop(sprintf("Fact table is not sub-aggregated and the `fun.aggregate` argument is not a function. Sub-aggregated your fact table or provide aggregate function to be used on all measures."))
        # - [x] sub-aggregate fact table
        key_cols = sapply(x$dims, key)
        x$fact[[fact]] = x$fact[[fact]][, lapply(.SD, fun.aggregate, ...), c(key_cols)]
    }
    cube$new(x)
}

# as.*.cube ---------------------------------------------------------------

as.array.cube = function(x, measure, ...){
    if(missing(measure)){
        fact_colnames = names(x$env$fact[[x$fact]])
        measure = fact_colnames[!fact_colnames %in% unlist(x$dapply(key))][1L]
    }
    as.array(x = x$env$fact[[x$fact]], dimnames = x$dapply(`[[`, 1L), measure = measure)
}

as.data.table.cube = function(x, na.fill = FALSE, dcast = FALSE, ...){
    r = x$denormalize(na.fill = na.fill)
    if(isTRUE(dcast)) dcast.data.table(r, ...) else r
}

as.list.cube = function(x, fact = "fact", ...){
    list(fact = setNames(list(x$env$fact[[x$fact]][]), fact), 
         dims = x$dapply(function(x) x))
}
