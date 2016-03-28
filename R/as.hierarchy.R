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
