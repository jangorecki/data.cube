#' @title Build dimension
#' @param x data.table or object with a \emph{as.data.table} method, build dimension based on that dataset.
#' @param \dots arguments passed to methods.
#' @param id.vars character scalar of dimension primary key.
#' @param hierarchies list of hierarchies of levels and their attributes.
#' @return dimension class object.
#' @seealso \code{\link{dimension}}, \code{\link{hierarchy}}, \code{\link{level}}, \code{\link{data.cube}}
#' @examples 
#' library(data.table)
#' time.dt = data.table(date = seq(as.Date("2015-01-01"), as.Date("2015-12-31"), by=1)
#'                      )[, c("month","quarter","year") := list(month(date), year(date), quarter(date))
#'                        ][, c("weekday","week") := list(weekdays(date), week(date))][]
#' hierarchies = list(
#'     "monthly" = list(
#'         "year" = character(),
#'         "quarter" = character(),
#'         "month" = character(),
#'         "date" = c("year","month")
#'     ),
#'     "weekly" = list(
#'         "year" = character(), 
#'         "week" = character(), 
#'         "weekday" = character(), 
#'         "date" = c("year","week","weekday")
#'     )
#' )
#' time = as.dimension(
#'     x = time.dt,
#'     id.vars = "date",
#'     hierarchies = hierarchies
#' )
#' str(time)
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
    ans = new.env()
    ans$data = data.table(NULL)
    ans$id.vars = character()
    ans$hierarchies = list()
    ans$levels = list()
    ans$fields = character()
    as.dimension.environment(ans)
}

# export

as.data.table.dimension = function(x, lvls = names(x$levels), ...) {
    stopifnot(is.dimension(x))
    ans = copy(x$data)
    lookupv(dims = lapply(x$levels[lvls], as.data.table.level), ans)
    ans[]
}
