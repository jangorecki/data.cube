#' @title Build cube
#' @param x R object.
#' @param \dots arguments passed to methods.
#' @param dimensions list of \link{dimension} class objects.
#' @param na.rm logical, default TRUE, when FALSE the cube would store cross product of all dimension grain keys!
#' @param id.vars characater vector of foreign key columns.
#' @param measure.vars characater vector of column names of metrics.
#' @param fun.aggregate function, default to \emph{sum}.
#' @param dims character vector of dimension names
#' @param hierarchies list of hierarchies nested in list of dimensions passed to \link{as.dimension}.
#' @param measures list of \link{measure} class objects passed to \link{as.fact}.
#' @return data.cube class object.
#' @seealso \code{\link{data.cube}}, \code{\link{fact}}, \code{\link{dimension}}
#' @examples 
#' library(data.table)
#' set.seed(1L)
#' dt = CJ(color = c("green","yellow","red"),
#'         year = 2011:2015, 
#'         status = c("active","inactive","archived","removed"))[sample(30)]
#' dt[, "value" := sample(4:7/2, nrow(dt), TRUE)]
#' 
#' # from data.table
#' dc = as.data.cube(
#'     x = dt, id.vars = c("color","year","status"),
#'     measure.vars = "value",
#'     hierarchies = sapply(c("color","year","status"),
#'                          function(x) list(setNames(list(character()), x)),
#'                          simplify=FALSE)
#' )
#' str(dc)
#' 
#' # multidimensional hierarchical data from fact and dimensions
#' X = populate_star(N = 1e3)
#' sales = X$fact$sales
#' time = X$dims$time
#' geography = X$dims$geography
#' # define hierarchies
#' time.hierarchies = list( # 2 hierarchies in time dimension
#'     "monthly" = list(
#'         "time_year" = character(),
#'         "time_quarter" = c("time_quarter_name"),
#'         "time_month" = c("time_month_name"),
#'         "time_date" = c("time_month","time_quarter","time_year")
#'     ),
#'     "weekly" = list(
#'         "time_year" = character(),
#'         "time_week" = character(),
#'         "time_date" = c("time_week","time_year")
#'     )
#' )
#' geog.hierarchies = list( # 1 hierarchy in geography dimension
#'     list(
#'         "geog_region_name" = character(),
#'         "geog_division_name" = c("geog_region_name"),
#'         "geog_abb" = c("geog_name","geog_division_name","geog_region_name")
#'     )
#' )
#' # create dimensions
#' dims = list(
#'     time = as.dimension(x = time, 
#'                         id.vars = "time_date", 
#'                         hierarchies = time.hierarchies),
#'     geography = as.dimension(x = geography, 
#'                              id.vars = "geog_abb", 
#'                              hierarchies = geog.hierarchies)
#' )
#' # create fact
#' ff = as.fact(
#'     x = sales,
#'     id.vars = c("geog_abb","time_date"),
#'     measure.vars = c("amount","value"),
#'     fun.aggregate = sum,
#'     na.rm = TRUE
#' )
#' # create data.cube
#' dc = as.data.cube(ff, dims)
#' str(dc)
as.data.cube = function(x, ...) {
    UseMethod("as.data.cube")
}

#' @rdname as.data.cube
#' @method as.data.cube default
as.data.cube.default = function(x, ...) {
    # this is local extension of base::as.array.default, which add ability to provide names for each of dimension, passed as `dims = c("A","B")`, useful to manually recreate name of dropped dimension
    as.array.default = function (x, dims) {
        if (is.array(x)) 
            return(x)
        n <- names(x)
        dim(x) <- length(x)
        if (missing(dims))
            dims = as.character(seq_along(dim(x)))
        if (length(n)) 
            dimnames(x) <- setattr(list(n), "names", dims)
        return(x)
    }
    as.data.cube.array(as.array(x, ...))
}

#' @rdname as.data.cube
#' @method as.data.cube matrix
as.data.cube.matrix = function(x, na.rm = TRUE, ...) {
    stopifnot(is.matrix(x), is.logical(na.rm))
    as.data.cube.array(x, na.rm=na.rm, ...=...)
}

#' @rdname as.data.cube
#' @method as.data.cube array
as.data.cube.array = function(x, na.rm = TRUE, ...) {
    stopifnot(is.array(x), is.logical(na.rm)) # process matrix also!
    ar.dim = dim(x)
    ar.dimnames = dimnames(x)
    if (is.null(names(ar.dimnames))) {
        setattr(attributes(x)$dimnames, "names", as.character(seq_along(ar.dimnames)))
        ar.dimnames = dimnames(x)
    }
    dt = as.data.table.array(x, na.rm = na.rm) # added .array explicitly cause .matrix is redirected here
    ff = as.fact.data.table(dt, id.vars = key(dt), measure.vars = "value", ...)
    if (any(dn.nulls <- sapply(ar.dimnames, is.null))) { # decode NULL in dimnames to character(0) for proper handling in `as.dimension`
        ar.dimnames[dn.nulls] = lapply(1:sum(dn.nulls), function(i) character(0))
    }
    dd = sapply(names(ar.dimnames), function(nm) {
        as.dimension(as.data.table(ar.dimnames[nm]),
                     id.vars = nm,
                     hierarchies = list(setNames(list(character(0)), nm)))
    }, simplify=FALSE)
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
as.data.cube.data.table = function(x, id.vars = key(x), measure.vars, fun.aggregate = sum, dims = id.vars, hierarchies = NULL, ..., dimensions, measures = NULL) {
    sub.fun = substitute(fun.aggregate)
    stopifnot(is.data.table(x), is.character(id.vars), is.character(measure.vars))
    if (!is.null(dims)) stopifnot(is.character(dims), length(dims) == length(id.vars))
    if (!is.null(hierarchies)) stopifnot(is.list(hierarchies), identical(names(hierarchies), id.vars) | identical(names(hierarchies), dims))
    if (!missing(dimensions)) stopifnot(sapply(dimensions, is.dimension))
    if (!missing(measures)) stopifnot(sapply(measures, is.measure))
    ff = eval(substitute(as.fact.data.table(x, id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = .fun.aggregate, ... = ..., measures = measures),
                         list(.fun.aggregate = sub.fun)))
    dims.id.vars = setNames(as.list(id.vars), dims)
    dd = lapply(setNames(nm = dims), function(nm) as.dimension.data.table(x, id.vars = dims.id.vars[[nm]], hierarchies = hierarchies[[nm]]))
    as.data.cube.fact(ff, dd)
}

#' @rdname as.data.cube
#' @method as.data.cube cube
as.data.cube.cube = function(x, hierarchies = NULL, ...){
    id.vars = key(x$env$fact[[x$fact]])
    if (length(hierarchies) && (length(hierarchies) != length(id.vars))) stop("list provided to 'hierarchies' argument must correspond to 'id.vars' dimensions")
    ff = x$fapply(as.fact.data.table, id.vars = id.vars)[[x$fact]]
    di = setNames(seq_along(x$dims), x$dims)
    dd = lapply(di, function(i) as.dimension.data.table(x$env$dims[[i]], id.vars = id.vars[i], hierarchies = hierarchies[[i]]))
    as.data.cube.fact(ff, dd)
}

# export

as.data.table.data.cube = function(x, na.fill = FALSE, dcast = FALSE, ...) {
    stopifnot(is.data.cube(x), is.logical(na.fill), is.logical(dcast))
    r = x$denormalize(na.fill = na.fill)
    if (isTRUE(dcast)) dcast.data.table(r, ...) else r
}

as.array.data.cube = function(x, measure, na.fill = NA, ...) {
    if (missing(measure)) measure = x$fact$measure.vars[1L]
    if (length(measure) > 1L) stop("Your cube seems to have multiple measures, you must provide scalar column name as 'measure' argument to as.array.")
    dimcols = lapply(x$dimensions, function(x) x$id.vars)
    stopifnot(sapply(dimcols, length) == 1L) # every key should be a single column key
    r = as.array.data.table(x = x$fact$data, dimcols = unlist(dimcols), measure = measure, dimnames = dimnames(x))
    # below will be removed after data.table#857 resolved, logic will goes into cb$denormalize method
    if (!is.na(na.fill) || is.nan(na.fill)) r[is.na(r)] = na.fill # cannot use is.na to track NaN also
    r
}
