#' @title Define measure
#' @param x character scalar column name of a measure.
#' @param label character scalar, a label for a measure.
#' @param fun.aggregate function, default \code{sum}.
#' @param \dots arguments passed to \emph{fun.aggregate}.
#' @param fun.format function to be used for formatting of a measure, such as \code{\link{currency.format}}.
#' @return measure class object.
#' @seealso \code{\link{fact}}, \code{\link{data.cube}}
as.measure = function(x, label = character(), fun.aggregate = sum, ..., fun.format = NULL){
    sub.fun = substitute(fun.aggregate)
    eval(substitute(measure$new(x, label = label, fun.aggregate = .fun.aggregate, ... = ..., fun.format = fun.format),
                    list(.fun.aggregate = sub.fun)))
}
