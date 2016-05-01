#' @title Create hierarchy
#' @param x list which keeps named character vectors (often length 0L) of level key and dependent attributes not listed yet on lower level key.
#' @param \dots arguments passed to methods.
#' @details You generally won't need to use that function directly, but use \code{\link{as.dimension}} that takes a list of (possibly named) hierarchies inputs.
#' @return hierarchy class object.
#' @seealso \code{\link{hierarchy}}, \code{\link{level}}, \code{\link{dimension}}, \code{\link{data.cube}}
#' @examples 
#' time.calendar = as.hierarchy(list(
#'   year = character(),
#'   quarter = character(),
#'   month = character(),
#'   date = c("year","quarter","month")
#' ))
#' str(time.calendar)
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

# export

as.list.hierarchy = function(x, lvls = names(x$levels), ...) {
    stopifnot(is.hierarchy(x))
    x$levels[lvls]
}
