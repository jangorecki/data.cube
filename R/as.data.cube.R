#' @title Build cube
#' @param x R object.
#' @param \dots arguments passed to methods.
#' @param dimensions list of \link{dimension} class objects.
#' @param na.rm logical, default TRUE, when FALSE the cube would store cross product of all dimension grain keys!
#' @param id.vars characater vector of foreign key columns.
#' @param measure.vars characater vector of column names of metrics.
#' @param fun.aggregate character function name, default to \emph{sum}.
#' @param dims character vector of dimension names
#' @param hierarchies list of hierarchies nested in list of dimensions passed to \link{as.dimension}.
#' @param measures list of \link{measure} class objects passed to \link{as.fact}.
#' @return data.cube class object.
as.data.cube = function(x, ...) {
    UseMethod("as.data.cube")
}

#' @rdname as.data.cube
#' @method as.data.cube default
as.data.cube.default = function(x, ...) {
    as.data.cube.array(as.array(x, ...))
}

as.data.cube.matrix = function(x, na.rm = TRUE, ...) {
    stopifnot(is.matrix(x), is.logical(na.rm))
    browser()
    stop("TODO dev")
    # multidimensional version of as.data.table.matrix, different than data.table::as.data.table.matrix
    as.data.table.matrix = function(x, na.rm = TRUE) {
        
    }
    ar.dimnames = dimnames(x)
    dt = as.data.table.matrix(x, na.rm = na.rm)
    ff = as.fact(dt, id.vars = key(dt), measure.vars = "value")
    dd = lapply(setNames(nm = names(ar.dimnames)), function(nm) {
        as.dimension(as.data.table(setNames(list(ar.dimnames[[nm]]), nm)),
                     id.vars = nm,
                     hierarchies = list(setNames(list(character(0)), nm)))
        
    })
    as.data.cube.fact(ff, dd)
}

#' @rdname as.data.cube
#' @method as.data.cube array
as.data.cube.array = function(x, na.rm = TRUE, ...) {
    stopifnot(is.array(x), is.logical(na.rm))
    ar.dimnames = dimnames(x)
    dt = as.data.table(x, na.rm = na.rm)
    ff = as.fact(dt, id.vars = key(dt), measure.vars = "value")
    dd = lapply(setNames(nm = names(ar.dimnames)), function(nm) {
        as.dimension(as.data.table(setNames(list(ar.dimnames[[nm]]), nm)),
                     id.vars = nm,
                     hierarchies = list(setNames(list(character(0)), nm)))
        
    })
    as.data.cube.fact(ff, dd)
}

#' @rdname as.data.cube
#' @method as.data.cube fact
as.data.cube.fact = function(x, dimensions = list(), ...) {
    stopifnot(is.list(dimensions), all(sapply(dimensions, is.dimension)), is.fact(x))
    data.cube$new(x, dimensions)
}

as.data.cube.environment = function(x, ...) {
    stopifnot(is.environment(x))
    data.cube$new(.env = x)
}

#' @rdname as.data.cube
#' @method as.data.cube list
as.data.cube.list = function(x, ...) {
    stopifnot(
        is.list(x),
        c("fact","dims","hierarchies") %in% names(x),
        length(id.vars <- sapply(x$dims, key)) == length(x$dims),
        length(fact.fields <- copy(names(x$fact[[names(x$fact)]]))) > 1L
    )
    dd = lapply(
        setNames(seq_along(x$dims), names(x$dims)),
        function(i) as.dimension(x$dims[[i]], hierarchies = x$hierarchies[[i]])
    )
    ff = as.fact(
        x = x$fact[[names(x$fact)]],
        id.vars = id.vars,
        measure.vars = setdiff(fact.fields, id.vars),
        ... = ...
    )
    as.data.cube.fact(ff, dd)
}

#' @rdname as.data.cube
#' @method as.data.cube data.table
as.data.cube.data.table = function(x, id.vars = key(x), measure.vars, fun.aggregate = "sum", dims = id.vars, hierarchies = NULL, ..., dimensions, measures) {
    stopifnot(is.data.table(x), is.character(id.vars), is.character(measure.vars), is.character(fun.aggregate))
    if (!is.null(dims)) stopifnot(is.character(dims), length(dims) == length(id.vars))
    if (!is.null(hierarchies)) stopifnot(is.list(hierarchies), identical(names(hierarchies), id.vars) | identical(names(hierarchies), dims))
    if (!missing(dimensions)) stopifnot(sapply(dimensions, is.dimension))
    if (!missing(measures)) stopifnot(sapply(measures, is.measure))
    ff = as.fact.data.table(x, id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = fun.aggregate, ... = ..., measures = measures)
    dims.id.vars = setNames(as.list(id.vars), dims)
    dd = lapply(setNames(nm = dims), function(nm) as.dimension.data.table(x, id.vars = dims.id.vars[[nm]], hierarchies = hierarchies[[nm]]))
    as.data.cube.fact(ff, dd)
}

as.data.cube.cube = function(x, hierarchies = NULL, ...){
    id.vars = key(x$env$fact[[x$fact]])
    ff = x$fapply(as.fact.data.table, id.vars = id.vars)[[x$fact]]
    di = setNames(seq_along(x$dims), x$dims)
    dd = lapply(di, function(i) as.dimension.data.table(x$env$dims[[i]], id.vars = id.vars[i], hierarchies = hierarchies[[i]]))
    as.data.cube.fact(ff, dd)
}
