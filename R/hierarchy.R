
# hierarchy ----

#' @title Hierarchy class
#' @docType class
#' @format An R6 class object.
#' @details Class stores set of dimension levels into hierarchy. Currently just keeps a list.
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
        }
    )
)
