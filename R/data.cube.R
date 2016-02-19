# stats collector for data.table
schema.data.table = function(x, empty){
    dm = dim(x)
    mb = as.numeric(object.size(x))/(1024^2)
    adr = as.character(address(x))[1L]
    ky = if(haskey(x)) paste(key(x), collapse=", ") else NA_character_
    dt = data.table(nrow = dm[1L], ncol = dm[2L], mb = mb, address = adr, sorted = ky)
    nn = copy(names(dt))
    if(!missing(empty)) setcolorder(dt[, c(empty) := NA], unique(c(empty, nn)))
    dt
}

# level ----

#' @title Level class
#' @docType class
#' @format An R6 class object.
#' @details Class stores lower grain of dimension attributes. Initialized with key column and propoerties columns - all depndent on the key.
level = R6Class(
    classname = "level",
    public = list(
        key = character(),
        properties = character(),
        data = NULL,
        initialize = function(x, key = key(x), properties){
            stopifnot(is.data.table(x), is.character(key), key %in% names(x), is.character(properties), properties %in% names(x))
            self$key = key
            rm(key)
            self$properties = unique(properties)
            self$data = setkeyv(unique(x, by = self$key)[, j = .SD, .SDcols = unique(c(self$key, self$properties))], self$key)[]
            invisible(self)
        },
        schema = function(){
            schema.data.table(self$data)
        },
        head = function(n = 6L){
            head(self$data, n)
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
        initialize = function(levels){ # x, key = key(x), 
            stopifnot(is.list(levels), as.logical(length(levels)))
            self$levels = levels
            invisible(self)
        }
    )
)

# dimension ----

#' @title Dimension class
#' @docType class
#' @format An R6 class object.
#' @details Class stores set of hierarchies. Initialized with hierarchies list definition. Also stores mapping from primary key to any level, to use snowflake
dimension = R6Class(
    classname = "dimension",
    public = list(
        key = character(),
        hierarchies = list(),
        levels = list(),
        data = NULL,
        initialize = function(x, key = key(x), hierarchies){
            stopifnot(is.data.table(x), is.character(key), key %in% names(x), is.list(hierarchies), as.logical(length(hierarchies)))
            stopifnot(
                # level keys in data.table
                (all.hierarchies.level.keys <- unique(unlist(lapply(hierarchies, names)))) %in% names(x),
                # level attributes in data.table
                (all.hierarchies.level.attrs <- unique(unname(unlist(hierarchies, recursive = TRUE)))) %in% names(x)
            )
            self$key = key
            rm(key)
            self$hierarchies = lapply(hierarchies, function(levels) hierarchy$new(levels = levels))
            # combine by levels
            common.levels = Reduce(f = function(x, y){
                lapply(setNames(nm = unique(c(names(x), names(y)))),
                       function(nm) c(x[[nm]], y[[nm]]))
            }, hierarchies)
            self$levels =  lapply(setNames(nm = names(common.levels)), function(lvlk){
                level$new(x, key = lvlk, properties = common.levels[[lvlk]])
            })
            # all.hierarchies.level.mappings
            granularity = unique(c(self$key, all.hierarchies.level.keys))
            self$data = setkeyv(unique(x, by = granularity)[, .SD, .SDcols = granularity], self$key)[]
            invisible(self)
        },
        schema = function(){ # for each dimensions
            i = setNames(seq_along(self$levels), names(self$levels))
            levels_schema = rbindlist(lapply(i, function(i) self$levels[[i]]$schema()), idcol = "entity")
            dimension_data_schema = schema.data.table(self$data, empty = c("entity"))
            rbindlist(list(dimension_data_schema, levels_schema))
        },
        head = function(n = 6L){
            list(base = head(self$data, n), levels = lapply(self$levels, function(x) x$head(n = n)))
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
        initialize = function(x, label = character(), fun.aggregate = "sum", ..., fun.format = function(x) x){
            self$dots = match.call(expand.dots = FALSE)$`...`
            self$var = x
            self$label = label
            self$fun.aggregate = fun.aggregate
            self$fun.format = fun.format
            invisible(self)
        },
        expr = function(){
            as.call(c(
                list(as.name(self$fun.aggregate), as.name(self$var)),
                self$dots
            ))
        },
        print = function(){
            cat(deparse(self$expr(), width.cutoff = 500L), sep="\n")
            invisible(self)
        }
    )
)

# fact ----

#' @title Fact class
#' @docType class
#' @format An R6 class object.
#' @details Class stores fact table as local data.table or remote big.data.table. Initialized with data.table or list of R nodes connections. Measures can be provided manually to `measures` argument, useful for custom aggregate function per measure.
fact = R6Class(
    classname = "fact",
    public = list(
        local = logical(),
        id.vars = character(), # foreign keys
        measure.vars = character(),
        measures = list(),
        data = NULL,
        initialize = function(x, id.vars = character(), measure.vars = character(), fun.aggregate = "sum", ..., measures){
            stopifnot(is.character(id.vars), is.character(measure.vars), is.character(fun.aggregate))
            self$id.vars = id.vars
            # - [x] `fact$new` creates measures, or use provided in `measures` argument
            if(missing(measures)){
                self$measure.vars = measure.vars
                self$measures = lapply(setNames(nm = self$measure.vars), function(var) measure$new(var, fun.aggregate = fun.aggregate, ... = ...))
            } else {
                self$measure.vars = names(self$measures)
                self$measures = measures
            }
            stopifnot(
                sapply(self$measures, inherits, "measure"),
                TRUE
                #unlist(lapply(self$measures, `[[`, "var")) %in% names(x) # 'x' not yet ready to use due to remote interface dev here
            )
            # build `j` expression
            jj = self$build.j()
            # aggregate
            dtq = substitute(x <- x[, j = .jj,, keyby = .id.vars], list(.jj = jj, .id.vars = self$id.vars))
            self$local = is.data.table(x)
            if(self$local){
                self$data = eval(dtq)
            } else {
                stopifnot(requireNamespace("big.data.table", quietly = TRUE), big.data.table::is.rscl(x))
                bdt = big.data.table::as.big.data.table(x)
                bdt[[expr = dtq, lazy = FALSE, send = TRUE]]
                self$data = bdt
            }
            invisible(self)
        },
        build.j = function(measure.vars = self$measure.vars){
            measure.which = sapply(self$measures, function(x) x$var %in% measure.vars)
            jj = as.call(c(
                list(as.name("list")),
                lapply(self$measures[measure.which], function(x) x$expr())
            ))
            if(isTRUE(getOption("datacube.jj"))) message(paste(deparse(jj, width.cutoff = 500), collapse = "\n"))
            jj
        },
        query = function(i, i.dt, by, measure.vars = self$measure.vars){
            
            ii = substitute(i)
            jj = self$build.j(measure.vars)
            bb = substitute(by)
            id = substitute(i.dt)
            
            l = list(
                as.symbol("["),
                x = call("$", as.name("self"), as.name("data")) # this can point to data.table or big.data.table
            )
            stopifnot(!(!missing(i) & !missing(i.dt)))
            if(!missing(i)){
                l[["i"]] = ii
            } else if(!missing(i.dt)){
                l[["i"]] = id
            }
            l[["j"]] = jj
            if(!missing(by)) l[["by"]] = bb
            if(!missing(i.dt)){
                jn = copy(names(i.dt))
                l[["on"]] = setNames(nm = jn)
                l[["nomatch"]] = 0L
            }
            dcq = as.call(l)
            dt = eval(dcq)
            if( !self$local ) {
                # re-aggr
                dcq["i"] = NULL
                dcq["on"] = NULL
                dcq["nomatch"] = 0L
                dcq[["x"]] = as.name("dt")
                dt = eval(dcq)
            }
            dt
        },
        schema = function(){
            schema.data.table(self$data, empty = c("entity"))
        },
        head = function(n = 6L){
            head(self$data, n)
        }
    )
)

# data.cube ----

#' @title Data.cube class
#' @docType class
#' @format An R6 class object.
#' @details Class stores fact class and dimension classes.
data.cube = R6Class(
    classname = "data.cube",
    public = list(
        fact = NULL,
        keys = character(),
        dimensions = list(),
        initialize = function(fact, dimensions){
            stopifnot(is.fact(fact), sapply(dimensions, is.dimension))
            self$dimensions = dimensions
            self$keys = lapply(self$dimensions, `[[`, "key")
            self$fact = fact
            invisible(self)
        },
        query = function(){
            NULL
        },
        index = function(){
            NULL
        },
        schema = function(){
            rbindlist(list(
                fact = rbindlist(list(fact = self$fact$schema()), idcol = "name"),
                dimension = rbindlist(lapply(self$dimensions, function(x) x$schema()), idcol = "name")
            ), idcol = "type")
        },
        print = function(){
            dict = self$schema()
            prnt = character()
            prnt["header"] = "<data.cube>"
            #prnt["distributed"] = if(!self$fact$local) sprintf("distributed: %s", NA_integer_)
            n.measures = length(dc$fact$measure.vars)
            prnt["fact"] = dict[type=="fact", sprintf("fact:\n  %s rows x %s dimensions x %s measures (%.2f MB)", nrow, ncol - n.measures, n.measures, mb)]
            if(length(self$dimensions)){
                dt = dict[type=="dimension", .(nrow = nrow[is.na(entity)], ncol = ncol[is.na(entity)], mb = sum(mb, na.rm = TRUE)), .(name)]
                prnt["dims"] = paste0("dimensions:\n", paste(dt[, sprintf("  %s : %s entities x %s levels (%.2f MB)", name, nrow, ncol, mb)], collapse="\n"))
            }
            prnt["size"] = sprintf("total size: %.2f MB", dict[,sum(mb)])
            cat(prnt, sep = "\n")
            invisible(self)
        },
        head = function(n = 6L){
            list(fact = self$fact$head(n = n), dimensions = lapply(self$dimensions, function(x) x$head(n = n)))
        }
    )
)

# is.* ----

#' @title Test if data.cube class
#' @param x object to tests.
is.data.cube = function(x) inherits(x, "data.cube")

#' @title Test if fact class
#' @param x object to tests.
is.fact = function(x) inherits(x, "fact")

#' @title Test if dimension class
#' @param x object to tests.
is.dimension = function(x) inherits(x, "dimension")

#' @title Test if measure class
#' @param x object to tests.
is.measure = function(x) inherits(x, "measure")

# as.* ----

#' @title Build dimension
#' @param x data.table build dimension based on that dataset.
#' @param key character scalar of dimension's primary key column.
#' @param hierarchies list of hierarchies and levels within dimensions.
#' @return dimension class object.
as.dimension = function(x, key = key(x), hierarchies){
    stopifnot(is.list(hierarchies), is.character(key), length(key) > 0L)
    dimension$new(x = x, key = key, hierarchies = hierarchies)
}

#' @title Build fact
#' @param x data.table build dimension based on that dataset.
#' @param id.vars character vector of all dimension's foreign keys.
#' @param measure.vars character vector, column names of measures.
#' @param fun.aggregate default "sum".
#' @param \dots arguments to fun.aggregate.
#' @param measures list of measures class objects, useful if various measures needs to have different `fun.aggregate`.
#' @return fact class object.
as.fact = function(x, id.vars = character(), measure.vars = character(), fun.aggregate = "sum", ..., measures){
    fact$new(x, id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = fun.aggregate, ... = ..., measures = measures)
}

#' @title Build cube
#' @param x fact class object.
#' @param dimensions list of dimension class objects.
#' @param \dots ignored.
#' @return fact class object.
as.data.cube = function(x, dimensions, ...){
    stopifnot(
        is.list(dimensions),
        sapply(dimensions, is.dimension),
        is.fact(x)
    )
    data.cube$new(x, dimensions)
}

# as.data.cube.function = function(){
#     # remote
#     
# }

# as.data.cube.data.table = function(x){
#     stopifnot(is.data.table(x))
#     # local
# 
# }

# data.cube methods `[`, `[[` ----

# @title Subset cube
# @param x data.cube object
# @param ... values to subset on corresponding dimensions, when wrapping in list it will refer to dimension hierarchy
# @param drop logical, default TRUE, drop dimensions same as *drop* argument in `[.array`.
# @return data.cube class object
# "[.data.cube" = function(x, ..., drop = TRUE){
#     if(!is.logical(drop)) stop("`drop` argument to cube subset must be logical. If argument name conflicts with your dimension name then provide it without name, elements in ... are matched by positions - as in array method - not names.")
#     r = x$subset(.dots = match.call(expand.dots = FALSE)$`...`)
#     if(isTRUE(drop)) r$drop() else r
#     r
# }

# @title Extract cube
# @param x data.cube object
# @param i list of values used to slice and dice on cube
# @param j expression to evaluate on fact
# @param by expression/character vector to aggregate measures accroding to *j* argument.
# @return data.cube?? class object
# "[[.data.cube" = function(x, i, j, by){
#     r = x$extract(by = by, .call = match.call())
#     r
# }
