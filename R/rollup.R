#' @title Rollup generic method
#' @param x R object
#' @param \dots arguments passed to methods
#' @description Converts arguments to *cube* class by reducing dimensions, sub aggregates are provided as additional NA dimension value..
#' @return *cube* class object. Use `format` on results to print table and format measures.
rollup = function(x, ...){
    UseMethod("rollup")
}

# rollup on data.table, man: http://stackoverflow.com/a/32938770/2490497
rollup.data.table = function(x, j, by, .SDcols, levels=TRUE, ...){
    stopifnot(is.data.table(x), is.character(by), length(by) >= 2L)
    stopifnot(is.logical(levels) | is.numeric(levels))
    stopifnot(!"level" %in% by) # reserved
    if(is.logical(levels)){
        level = levels
        levels = c(0L,seq_along(by))
    } else if(is.numeric(levels)){
        levels = as.integer(levels)
        level = TRUE
    }
    if(missing(.SDcols)) .SDcols = character()
    j = substitute(j)
    aggrs = rbindlist(c(
        lapply(1:(length(by)-1L), function(i){
            if(i %in% levels){
                if(length(.SDcols)) x[, eval(j), c(by[1:i]), .SDcols = .SDcols][, (by[-(1:i)]) := NA][, c("level") := i]
                else x[, eval(j), c(by[1:i])][, (by[-(1:i)]) := NA][, c("level") := i]
            }
        }), # subtotals
        list({
            if(0L %in% levels){
                if(length(.SDcols)) x[, eval(j), c(by), .SDcols = .SDcols][, c("level") := 0L]
                else x[, eval(j), c(by)][, c("level") := 0L]
            }
        }), # leafs aggregations
        list({
            if(length(by) %in% levels){
                if(length(.SDcols)) x[, eval(j), .SDcols = .SDcols][, c(by) := NA][, c("level") := length(by)]
                else x[, eval(j)][, c(by) := NA][, c("level") := length(by)]
            }
        }) # grand total
    ), use.names = TRUE, fill = FALSE)
    if(!level) aggrs[, c("level") := NULL] else by = c(by, "level")
    setcolorder(aggrs, neworder = c(by, names(aggrs)[!names(aggrs) %in% by]))
    return(aggrs[])
}

# x cube
# MARGIN character vector to rollup `by`
# INDEX apply filter on levels where level 0 represents the most granular data, each next is an higher level aggregation. Having value of NULL and `length > 1L` will result double counting of measures, NULL recycles to length of dims.
# FUN function to apply on all measures
# ... more arguments passed to FUN
# j call passed to data.table fact table.
# normalize default TRUE will normalize multiple attributes from single dimension with new surrogate key
# drop use drop method on returning cube
rollup.cube = function(x, MARGIN, INDEX = NULL, FUN, ..., j, normalize = TRUE, drop = TRUE){
    keys = x$dapply(key, simplify = TRUE)
    measures = setdiff(names(x$env$fact[[x$fact]]), keys)
    levels = if(!is.null(INDEX)) INDEX else c(0L,seq_along(MARGIN))
    r = new.env()
    if(missing(FUN)){
        r$fact[[x$fact]] = eval(substitute(rollup(x = as.data.table(x), j = jj, by = MARGIN, levels = levels), env = list(jj = substitute(j))))
    } else {
        jj = as.call(list(quote(lapply), X = quote(.SD), FUN = substitute(FUN), "..." = ...))
        r$fact[[x$fact]] = eval(substitute(rollup(x = as.data.table(x), j = jj, by = MARGIN, .SDcols = measures, levels = levels), env = list(jj = jj)))
    }
    if(!normalize){
        keys = setdiff(names(r$fact[[x$fact]]), measures)
        if(length(keys)) rr = setorderv(r$fact[[x$fact]], cols = keys, order=1L, na.last=TRUE) else {
            stopifnot(nrow(r$fact[[x$fact]])==1L) # grant total
        }
        return(r$fact[[x$fact]])
    }
    new.fact.colnames = names(r$fact[[x$fact]])
    new.fact.keys = setdiff(new.fact.colnames, measures)
    copy.dims = dimnames.attributes(new.fact.keys[new.fact.keys!="level"], x)
    names(new.fact.keys) = c(rep(names(copy.dims), sapply(copy.dims, length)), "level")
    r$dims = lapply(selfNames(names(copy.dims)), function(dim){
        full.sd = names(x$env$dims[[dim]])
        new_natural_key = unname(new.fact.keys[names(new.fact.keys)==dim])
        dim.sd.i = chmatch(new_natural_key, full.sd)
        if(!length(dim.sd.i)) browser() #stop("No column in dimensions?")
        dim.sd = full.sd[min(dim.sd.i):length(full.sd)]
        r$dims[[dim]] = if(length(dim.sd.i) > 1L){
            new.dim = add.surrogate.key(unique(x$env$dims[[dim]][, dim.sd, with=FALSE], by = new_natural_key), paste(dim, "id", sep="_"))
            # decode multiple attributes from one dimension into new surrogate keys
            cols = copy(names(r$fact[[x$fact]]))
            drop_cols_i = chmatch(new_natural_key, cols)
            cols[drop_cols_i[1L]] = paste(dim, "id", sep="_")
            cols = cols[-drop_cols_i[-1L]]
            lookup(r$fact[[x$fact]], setkeyv(new.dim, new_natural_key), cols = paste(dim, "id", sep="_"))
            r$fact[[x$fact]][, c(new_natural_key) := NULL]
            setcolorder(r$fact[[x$fact]], cols)
            return(setkeyv(new.dim, paste(dim, "id", sep="_"))[])
        } else if(length(dim.sd.i)==1L){
            return(setkeyv(unique(x$env$dims[[dim]][, dim.sd, with=FALSE], by = new_natural_key), new_natural_key)[])
        }
    })
    r$dims[["level"]] = data.table(level = levels, key = "level")
    normalized.new.fact.keys = unlist(lapply(r$dims, key))
    setkeyv(r$fact[[x$fact]], unname(normalized.new.fact.keys))
    if(isTRUE(drop)) as.cube(r)$drop() else as.cube(r)
}
