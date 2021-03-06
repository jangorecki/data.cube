#' @title Create level
#' @param x data.table or object with a \emph{as.data.table} method, build level based on that dataset.
#' @param \dots arguments passed to methods.
#' @param id.vars character scalar of level primary key.
#' @param properties character vector of column names from dataset to include on that level.
#' @return level class object.
#' @details You generally won't need to use that function directly, but use \code{\link{as.dimension}} that based on hierarchies will produce all levels.
#' @seealso \code{\link{level}}, \code{\link{hierarchy}}, \code{\link{dimension}}, \code{\link{data.cube}}
#' @examples 
#' library(data.table)
#' time.dt = data.table(d = seq(as.Date("2015-01-01"), as.Date("2015-12-31"), by=1)
#'                      )[, c("m","y") := list(month(d), year(d))
#'                        ][]
#' date.level = as.level(time.dt, id.vars="d", properties=c("m","y"))
#' str(date.level)
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

# export

as.data.table.level = function(x, ...) {
    stopifnot(is.level(x))
    copy(x$data)
}
