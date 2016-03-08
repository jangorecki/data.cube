#' @title Build fact
#' @param x data.table build dimension based on that dataset.
#' @param id.vars character vector of all dimension's foreign keys.
#' @param measure.vars character vector, column names of measures.
#' @param fun.aggregate default "sum".
#' @param \dots arguments to fun.aggregate.
#' @param measures list of measures class objects, useful if various measures needs to have different `fun.aggregate`.
#' @return fact class object.
as.fact = function(x, ...){
    UseMethod("as.fact")
}

#' @rdname as.fact
#' @method as.fact default
as.fact.default = function(x, id.vars = character(), measure.vars = character(), fun.aggregate = "sum", ..., measures){
    if(is.null(x)) return(null.fact())
    as.fact.data.table(as.data.table(x, ...))
}

#' @rdname as.fact
#' @method as.fact data.table
as.fact.data.table = function(x, id.vars = as.character(key(x)), measure.vars = setdiff(names(x), id.vars), fun.aggregate = "sum", ..., measures){
    fact$new(x, id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = fun.aggregate, ... = ..., measures = measures)
}

#' @rdname as.fact
#' @method as.fact list
as.fact.list = function(x, id.vars = as.character(key(x)), measure.vars = setdiff(names(x), id.vars), fun.aggregate = "sum", ..., measures){
    stopifnot(requireNamespace("big.data.table", quietly = TRUE), big.data.table::is.rscl(x))
    fact$new(x, id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = fun.aggregate, ... = ..., measures = measures)
}

as.fact.environment = function(x, ...){
    fact$new(.env = x)
}

null.fact = function(...){
    env = new.env()
    env$local = TRUE
    env$id.vars = character()
    env$measure.vars = character()
    env$measures = list()
    env$data = data.table(NULL)
    as.fact.environment(env)
}
