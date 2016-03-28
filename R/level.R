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
            r = unique(x, by = self$id.vars)[, j = .SD, .SDcols = unique(c(self$id.vars, self$properties))]
            self$data = setkeyv(r, self$id.vars)[]
            invisible(self)
        },
        print = function() {
            lvl.data.str = capture.output(str(self$data, give.attr = FALSE))
            cat(c("<level>", lvl.data.str), sep="\n")
            invisible(self)
        },
        dim = function() {
            as.integer(nrow(self$data))
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

#' @title Test if level class
#' @param x object to tests.
is.level = function(x) inherits(x, "level")

names.level = function(x) names(x$data)
length.level = function(x) nrow(x$data)
dim.level = function(x) {
    stopifnot(is.level(x))
    x$dim()
}
