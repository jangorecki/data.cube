#' @title Dimension class
#' @docType class
#' @format An R6 class object.
#' @details Class stores set of hierarchies. Initialized with hierarchies list definition. Also stores mapping from primary key to any level, to use snowflake
#' @seealso \code{\link{as.dimension}}, \code{\link{hierarchy}}, \code{\link{level}}, \code{\link{fact}}, \code{\link{data.cube}}
dimension = R6Class(
    classname = "dimension",
    public = list(
        id.vars = character(),
        fields = character(),
        hierarchies = list(),
        levels = list(),
        data = NULL,
        initialize = function(x, id.vars = key(x), hierarchies, .env) {
            if(!missing(.env)) {
                # skip heavy processing for env argument
                self$data = .env$data # potentially null to be filled based on fact table
                self$id.vars = .env$id.vars
                self$hierarchies = .env$hierarchies
                self$levels = .env$levels
                self$fields = .env$fields
                return(invisible(self))
            }
            stopifnot(is.data.table(x), is.character(id.vars), id.vars %in% names(x), is.list(hierarchies), as.logical(length(hierarchies)))
            stopifnot(
                # level keys in data.table
                (all.hierarchies.level.keys <- unique(unlist(lapply(hierarchies, names)))) %in% names(x),
                # level attributes in data.table
                (all.hierarchies.level.attrs <- unique(unname(unlist(hierarchies, recursive = TRUE)))) %in% names(x)
            )
            self$id.vars = id.vars
            self$hierarchies = lapply(hierarchies, function(levels) as.hierarchy(levels))
            # combine by levels
            common.levels = Reduce(function(x, y) {
                lapply(setNames(nm = unique(c(names(x), names(y)))),
                       function(nm) c(x[[nm]], y[[nm]]))
            }, hierarchies)
            self$levels = lapply(setNames(nm = names(common.levels)), function(lvlk) {
                as.level(x, id.vars = lvlk, properties = common.levels[[lvlk]])
            })
            dt = rbindlist(lapply(names(self$levels), function(lvlk) data.table(properties = c(lvlk, self$levels[[lvlk]]$properties))[, level := lvlk]))[, tail(.SD, 1L), properties]
            self$fields = dt$properties # TEST on error use: setNames(dt$properties, dt$level)
            # all.hierarchies.level.mappings
            granularity = unique(c(self$id.vars, all.hierarchies.level.keys))
            r = unique(x, by = granularity)[, j = .SD, .SDcols = granularity]
            self$data = setkeyv(r, self$id.vars)[]
            invisible(self)
        },
        print = function() {
            dimension.data.str = capture.output(str(self$data, give.attr = FALSE))
            cat(c("<dimension>", dimension.data.str), sep="\n")
            invisible(self)
        },
        schema = function() { # for each dimensions
            i = setNames(seq_along(self$levels), names(self$levels))
            levels_schema = rbindlist(lapply(i, function(i) self$levels[[i]]$schema()), idcol = "entity")
            dimension_data_schema = schema.data.table(self$data, empty = c("entity"))
            rbindlist(list(dimension_data_schema, levels_schema))
        },
        head = function(n = 6L) {
            list(base = head(self$data, n), levels = lapply(self$levels, function(x) x$head(n = n)))
        },
        # subset
        subset = function(i.sub) {
            stopifnot(is.list(i.sub))
            if (identical(i.sub, vector("list"))) return(self) # for `list()` returns self
            r = new.env()
            # - [ ] iterate over levels in a dimension to subset those which are using in filter
            filter.cols = names(i.sub)
            filter.lvls = sapply(self$levels, function(x) any(filter.cols %chin% c(x$id.vars, x$properties)))
            lvls.subs = sapply(self$levels[filter.lvls], function(x) {
                lvl.filter.cols = filter.cols[filter.cols %chin% c(x$id.vars, x$properties)]
                if(!length(lvl.filter.cols)) browser() # already excluded in sapply X arg
                # - [x] list handled in level
                x$subset(i.sub[lvl.filter.cols])
            }, simplify=FALSE)
            # - [ ] subset base dimension update
            ii = sapply(lvls.subs, function(level) {
                self$data[level$data, on=level$id.vars, which=TRUE, nomatch=0L]
            }, simplify=FALSE)
            ii = Reduce(intersect, ii) # intersection of `which=TRUE`, change if OR `|` operator supported
            r$data = self$data[ii]
            stopifnot(is.data.table(r$data), ncol(r$data) > 0L)
            # now fetch all levels having dimension base filtered
            r$levels = sapply(self$levels, function(x) {
                x$subset(unique(r$data, by=x$id.vars))
            }, simplify=FALSE)
            r$id.vars = self$id.vars
            r$hierarchies = self$hierarchies
            r$fields = self$fields
            setkeyv(r$data, r$id.vars)
            as.dimension.environment(r)
        },
        setindex = function(drop = FALSE) {
            setindexv(self$data, if (!drop) names(self$data)) # this is base of a dimensions so all columns!
            lapply(self$levels, function(x) x$setindex(drop=drop))
            invisible(self)
        },
        rollup = function(x, i.ops) {
            stopifnot(is.character(i.ops))
            r = new.env()
            browser()
            if (is.list(x)) {
                stopifnot(names(x) %chin% names(self$hierarchies))
                r$hierarchies = sapply(self$hierarchies, function(h) h$rollup(x), simplify=FALSE)
                
            } else {
                
            }
        }
    )
)

#' @title Test if dimension class
#' @param x object to tests.
is.dimension = function(x) inherits(x, "dimension")

dimnames.dimension = function(x) {
    r = x$levels[[x$id.vars]]$data[[1L]]
    if (!length(r)) return(NULL)
    r
}

str.dimension = function(object, ...) {
    print(object$schema())
    invisible()
}

names.dimension = function(x) as.character(names(x$data))
length.dimension = function(x) as.integer(length(x$data))
dim.dimension = function(x) as.integer(dim(x$data))
