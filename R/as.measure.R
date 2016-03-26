#' @title Form measure
#' @param x character scalar column name of a measure.
#' @param label character scalar, a label for a measure.
#' @param fun.aggregate character scalar function name.
#' @param \dots arguments passed to \emph{fun.aggregate}.
#' @param fun.format function to be used for formatting of a measure, such as \code{\link{currency.format}}.
#' @return measure class object.
as.measure = function(x, label = character(), fun.aggregate = "sum", ..., fun.format = NULL){
    measure$new(x, label = label, fun.aggregate = fun.aggregate, ... = ..., fun.format = fun.format)
}
