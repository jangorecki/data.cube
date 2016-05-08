#' @title Hierarchy class
#' @docType class
#' @format An R6 class object.
#' @details Class stores set of dimension levels into hierarchy. Currently just keeps a list.
#' @seealso \code{\link{as.hierarchy}}, \code{\link{level}}, \code{\link{dimension}}, \code{\link{data.cube}}
hierarchy = R6Class(
    classname = "hierarchy",
    public = list(
        levels = list(),
        initialize = function(levels) {
            stopifnot(is.list(levels), as.logical(length(levels)))
            self$levels = levels
            invisible(self)
        },
        print = function() {
            hierarchy.str = capture.output(str(self$levels, give.attr = FALSE))
            cat(c("<hierarchy>", hierarchy.str), sep="\n")
            invisible(self)
        },
        rollup = function(x) {
            # all higher attributes in hierarchy are taken
            # if not used, it won't be calculated in facts
            
            
            browser()
        }
    )
)

#' @title Test if hierarchy class
#' @param x object to tests.
is.hierarchy = function(x) inherits(x, "hierarchy")

str.hierarchy = function(object, ...) {
    cat(str(as.list.hierarchy(object)))
    invisible()
}

names.hierarchy = function(x) as.character(names(x$levels))
length.hierarchy = function(x) as.integer(length(x$levels))
