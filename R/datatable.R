#' @title Convert array to data.table
#' @param x array
#' @param na.rm logical default TRUE, `NA` value of a measure is not included into resulting data.table
#' @param \dots ignored
#' @note Array should not have a dimension named *value* because this name will be used for a measure in data.table.
#' @return A data.table object with (by default) non-NA values of a measure for each dimension cross.
#' @method as.data.table array
as.data.table.array = function(x, na.rm=TRUE, ...){
    dims = names(dimnames(x))
    if("value" %in% dims) stop("Array to convert must not already have `value` character as dimension name. `value` name is reserved to converted measure, rename dimname of input array.")
    r = do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", dims), as.symbol)))),, keyby = c(dims)]
    if(na.rm) r[!is.na(value)] else r
}

#' @title Convert data.table to array
#' @description Converts single measure of data.table into array dimensioned by columns provided as argument.
#' @param x data.table
#' @param dimcols character vector of key column for all dimensions, if named then names will be used as dimension names, example `c(product = "prod_id", x = "x_id")`.
#' @param measure character scalar column name of a measure, is automatically detected if it is the only data.table columns not in *dimcols* or names of *dimnames*.
#' @param dimnames list of named dimension key values, the same as \link{array} *dimnames* argument. If missing then dimension key values will be computed from *x* data.table.
#' @param \dots ignored
#' @note When using *dimcols* arg the function will extract dimension key values from *x* data.table. If you have unique values for each dimension on hand, you can use *dimnames* argument to skip potentially heavy task of extract dimensions, also combine with *dimcols* if you need remap column name to custom dimension name.
#' @return An array, when using *dimnames* the length will be `prod(sapply(dimnames, length))`, when using *dimcols* only the length will be `x[, prod(sapply(.SD, uniqueN)), .SDcols = dimcols]`.
#' @method as.array data.table
as.array.data.table = function(x, dimcols, measure, dimnames, ...){
    stopifnot(!missing(dimcols) | !missing(dimnames))
    # zero dims fast escape
    if(!missing(dimcols) && !length(dimcols) && !missing(dimnames) && !length(dimnames)){
        if(length(x) > 1L) stopifnot(is.character(measure), length(measure)==1L, measure %in% names(x))
        return(x[[measure]])
    }
    if(!missing(dimcols)) stopifnot(is.character(dimcols), is.unique(names(dimcols)), is.unique(dimcols), dimcols %in% names(x))
    if(!missing(dimnames)) stopifnot(is.list(dimnames), !is.null(names(dimnames)), is.unique(names(dimnames)))
    if(!missing(dimcols) && !missing(dimnames)){
        if(length(dimcols) && is.null(names(dimcols))) dimcols = setNames(dimcols, names(dimnames))
    }
    if(missing(dimcols) && !missing(dimnames)){
        stopifnot(names(dimnames) %in% names(x))
        dimcols = selfNames(names(dimnames))
    }
    if(missing(measure)){
        if(length(x)==length(dimcols)+1L) measure = names(x)[length(x)] # if only one measure then use it as default for `measure` arg
    }
    stopifnot(is.character(measure), length(measure)==1L)
    if(!length(dimcols)) return(x[, eval(as.name(measure))])
    if(!missing(dimcols) && missing(dimnames)){
        if(is.null(names(dimcols))) names(dimcols) = dimcols
        dimnames = lapply(dimcols, function(dimcol) fsort(unique(x, by = dimcol)[[dimcol]])) # optimized sort
    }
    revkey = rev(unname(dimcols))
    if(nrow(x)){
        crossdims = quote(setkeyv(setnames(do.call(CJ, dimnames), dimcols), revkey))
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

is.unique = function(x){
    if(is.null(x)) return(TRUE) # fixed in 1.9.7 data.table#1429
    length(x)==uniqueN(x)
}

# fast check if data aggregated to all dimensions
is.unique.data.table = function(x, by = key(x)){
    if(is.null(x)) return(logical(0)) # fixed in 1.9.7 data.table#1429
    nrow(x)==uniqueN(x, by = by)
}

# not yet exported from data.table
fsort = data.table:::fsort

# used with data.table join `on` argument
swap.on = function(x){
    stopifnot(is.character(x), length(names(x))==length(x))
    structure(names(x), names = x)
}

# join and lookup chosen columns
lookup = function(fact, dim, cols){
    if(any(cols %in% names(fact))) stop(sprintf("Column name collision on lookup for '%s' columns.", paste(cols[cols %in% names(fact)], collapse=", ")))
    fact[dim, (cols) := mget(paste0("i.", cols)), on = c(key(dim))]
    # workaround for data.table#1166 - lookup NAs manually
    if(all(!cols %in% names(fact))) fact[, (cols) := as.list(dim[0L, cols, with=FALSE][1L])]
    TRUE
}
