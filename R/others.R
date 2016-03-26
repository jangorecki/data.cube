
# level ----

#' @title Level class
#' @docType class
#' @format An R6 class object.
#' @details Class stores lower grain of dimension attributes. Initialized with key column and propoerties columns - all depndent on the key.
level = R6Class(
    classname = "level",
    public = list(
        id.vars = character(),
        properties = character(),
        data = NULL,
        initialize = function(x, id.vars = key(x), properties) {
            stopifnot(is.data.table(x), is.character(id.vars), id.vars %in% names(x), is.character(properties), properties %in% names(x))
            self$id.vars = id.vars
            self$properties = unique(properties)
            self$data = setkeyv(unique(x, by = self$id.vars)[, j = .SD, .SDcols = unique(c(self$id.vars, self$properties))], self$id.vars)[]
            invisible(self)
        },
        print = function() {
            lvl.data.str = capture.output(str(self$data, give.attr = FALSE))
            cat(c("<level>", lvl.data.str), sep="\n")
            invisible(self)
        },
        schema = function() {
            schema.data.table(self$data)
        },
        head = function(n = 6L) {
            head(self$data, n)
        },
        subset = function(i) {
            if (is.language(i)) level$new(x = self$data[eval(i)], 
                                          id.vars = key(self$data), 
                                          properties = self$properties)
            else if (is.data.table(i)) level$new(x = self$data[i, .SD, .SDcols=c(self$id.vars, self$properties), nomatch=0L, on=self$id.vars], 
                                                 id.vars = key(self$data), 
                                                 properties = self$properties)
            else if (is.integer(i) && i == 0L) level$new(x = self$data[i], 
                                                         id.vars = key(self$data), 
                                                         properties = self$properties)
            else stop("unsupported 'i' argument")
        },
        setindex = function(drop = FALSE) {
            setindexv(self$data, if (!drop) names(self$data))
            invisible(self)
        }
    )
)

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

# measure ----

#' @title Measure class
#' @docType class
#' @format An R6 class object.
#' @details Class stores variable name from fact table, the function to use against variable. Initialized with character scalar variable name, optional label, function to use on aggregates and it's custom arguments. Method `format` is provided to produce clean expressions.
measure = R6Class(
    classname = "measure",
    public = list(
        var = character(),
        fun.aggregate = character(),
        fun.format = NULL,
        dots = list(),
        label = character(),
        initialize = function(x, label = character(), fun.aggregate = "sum", ..., fun.format = function(x) x) {
            self$dots = match.call(expand.dots = FALSE)$`...`
            self$var = x
            self$label = label
            self$fun.aggregate = fun.aggregate
            self$fun.format = fun.format
            invisible(self)
        },
        expr = function() {
            as.call(c(
                list(as.name(self$fun.aggregate), as.name(self$var)),
                self$dots
            ))
        },
        print = function() {
            cat(deparse(self$expr(), width.cutoff = 500L), sep="\n")
            invisible(self)
        }
    )
)

#' @title Test if measure class
#' @param x object to tests.
is.measure = function(x) inherits(x, "measure")
