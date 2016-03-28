#' @title Build dimension
#' @param x data.table or object with a \emph{as.data.table} method, build dimension based on that dataset.
#' @param \dots arguments passed to methods.
#' @param id.vars character scalar of dimension primary key.
#' @param hierarchies list of hierarchies of levels and its attributes.
#' @return dimension class object.
as.dimension = function(x, ...){
    UseMethod("as.dimension")
}

#' @rdname as.dimension
#' @method as.dimension default
as.dimension.default = function(x, id.vars, hierarchies = NULL, ...){
    if(is.null(x)) return(null.dimension())
    as.dimension.data.table(as.data.table(x), id.vars = id.vars, hierarchies = hierarchies)
}

#' @rdname as.dimension
#' @method as.dimension data.table
as.dimension.data.table = function(x, id.vars = key(x), hierarchies = NULL, ...){
    if(is.null(hierarchies)) hierarchies = list(setNames(rep(list(character(0)), length(id.vars)), id.vars))
    stopifnot(is.list(hierarchies), is.character(id.vars), length(id.vars) > 0L)
    dimension$new(x = x, id.vars = id.vars, hierarchies = hierarchies)
}

as.dimension.environment = function(x, ...){
    dimension$new(.env = x)
}

null.dimension = function(...){
    env = new.env()
    env$data = data.table(NULL)
    env$id.vars = character()
    env$hierarchies = list()
    env$levels = list()
    env$fields = character()
    as.dimension.environment(env)
}

# export

as.data.table.dimension = function(x, lvls = names(x$levels), ...) {
    stopifnot(is.dimension(x))
    r = copy(x$data)
    lookupv(dims = lapply(x$levels[lvls], as.data.table.level), r)
    r[]
}
