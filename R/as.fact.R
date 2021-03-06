#' @title Build fact
#' @param x data.table build dimension based on that dataset.
#' @param id.vars character vector of all dimension's foreign keys.
#' @param measure.vars character vector, column names of measures.
#' @param fun.aggregate function default \code{sum}.
#' @param \dots arguments to fun.aggregate.
#' @param measures list of measures class objects, useful if various measures needs to have different \code{fun.aggregate}.
#' @return fact class object.
#' @seealso \code{\link{fact}}, \code{\link{measure}}, \code{\link{dimension}}, \code{\link{data.cube}}
#' @examples 
#' library(data.table)
#' dt = data.table(a=rep(1:6,2), b=letters[1:3], d=letters[1:2], z=1:12*sin(1:12))
#' ff = as.fact(x = dt,
#'              id.vars = c("a","b","d"),
#'              measure.vars = "z")
#' str(ff)
as.fact = function(x, ...) {
    UseMethod("as.fact")
}

#' @rdname as.fact
#' @method as.fact default
as.fact.default = function(x, id.vars = character(), measure.vars = character(), fun.aggregate = sum, ..., measures = NULL) {
    sub.fun = substitute(fun.aggregate)
    if (is.null(x)) return(null.fact())
    eval(substitute(as.fact.data.table(as.data.table(x, ...), id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = .fun.aggregate, ... = ..., measures = measures),
                    list(.fun.aggregate = sub.fun)))
}

#' @rdname as.fact
#' @method as.fact data.table
as.fact.data.table = function(x, id.vars = as.character(key(x)), measure.vars = setdiff(names(x), id.vars), fun.aggregate = sum, ..., measures = NULL) {
    sub.fun = substitute(fun.aggregate)
    eval(substitute(fact$new(x, id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = .fun.aggregate, ... = ..., measures = measures),
                    list(.fun.aggregate = sub.fun)))
}

null.fact = function(...) {
    env = new.env()
    env$id.vars = character()
    env$measure.vars = character()
    env$measures = list()
    env$data = data.table(NULL)
    as.fact.environment(env)
}

# export

as.data.table.fact = function(x, ...) {
    stopifnot(is.fact(x))
    copy(x$data)
}

as.fact.environment = function(x, ...) {
    fact$new(.env = x)
}
