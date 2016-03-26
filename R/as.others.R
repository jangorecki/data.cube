#' @title Create level
#' @param x data.table or object with a \emph{as.data.table} method, build level based on that dataset.
#' @param \dots arguments passed to methods.
#' @param id.vars character scalar of level primary key.
#' @param properties character vector of column names from dataset to include on that levell.
#' @return level class object.
as.level = function(x, ...){
    UseMethod("as.level")
}

#' @rdname as.level
#' @method as.level default
as.level.default = function(x, id.vars = key(x), properties, ...){
    if(is.null(x)) return(null.level())
    as.level.data.table(as.data.table(x, ...), id.vars = id.vars, properties = properties)
}

#' @rdname as.level
#' @method as.level data.table
as.level.data.table = function(x, id.vars = key(x), properties, ...){
    stopifnot(is.character(properties), is.character(id.vars), length(id.vars) > 0L)
    level$new(x = x, id.vars = id.vars, properties = properties)
}

null.level = function(...){
    stop("null.level object is not allowed")
}

#' @title Create hierarchy
#' @param x list or object with a \emph{as.list} method.
#' @param \dots arguments passed to methods.
#' @return hierarchy class object.
as.hierarchy = function(x, ...){
    UseMethod("as.hierarchy")
}

#' @rdname as.hierarchy
#' @method as.hierarchy default
as.hierarchy.default = function(x, ...){
    if(is.null(x)) return(null.hierarchy())
    as.hierarchy.list(as.list(x, ...))
}

#' @rdname as.hierarchy
#' @method as.hierarchy list
as.hierarchy.list = function(x, ...){
    hierarchy$new(x)
}

null.hierarchy = function(...){
    stop("null hierarchy not allowed")
}
