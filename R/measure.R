#' @title Measure class
#' @docType class
#' @format An R6 class object.
#' @details Class stores variable name from fact table, the function to use against variable. Initialized with character scalar variable name, optional label, function to use on aggregates and it's custom arguments. Method `format` is provided to produce clean expressions.
measure = R6Class(
    classname = "measure",
    public = list(
        var = character(),
        fun.aggregate = NULL,
        fun.format = NULL,
        dots = list(),
        label = character(),
        initialize = function(x, label = character(), fun.aggregate = sum, ..., fun.format = NULL) {
            fun.aggregate = substitute(fun.aggregate)
            self$dots = match.call(expand.dots = FALSE)$`...`
            self$var = x
            self$label = label
            self$fun.aggregate = fun.aggregate
            self$fun.format = fun.format
            invisible(self)
        },
        expr = function() {
            as.call(c(
                list(self$fun.aggregate, as.name(self$var)),
                self$dots
            ))
        },
        print = function() {
            cat("<measure>", deparse(self$expr(), width.cutoff = 500L), sep="\n")
            invisible(self)
        }
    )
)

#' @title Test if measure class
#' @param x object to tests.
is.measure = function(x) inherits(x, "measure")

# predefined formatting funs ----

#' @title Function to format numeric as currency
#' @param x numeric vector to format.
#' @param currency.sym character default \code{"$"}.
#' @param sym.align character default \code{"left"}, alternatively \code{"right"}.
#' @param digits precision in rounding
#' @param sep character separator for thousands, default \code{","}.
#' @param decimal character separator for decimal places, default \code{"."}.
#' @param \dots ignored.
#' @return character vector of the same length as input formatted as defined currency.
currency.format = function(x, currency.sym="$", sym.align="left", digits=2, sep=",", decimal=".", ...) {
    paste0(if(sym.align=="left") currency.sym,
           formatC(x, format = "f", big.mark = sep, digits=digits, decimal.mark=decimal),
           if(sym.align=="right") currency.sym)
}