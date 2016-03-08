#' @title Dimension class
#' @docType class
#' @format An R6 class object.
#' @details Class stores set of hierarchies. Initialized with hierarchies list definition. Also stores mapping from primary key to any level, to use snowflake
dimension = R6Class(
    classname = "dimension",
    public = list(
        id.vars = character(),
        fields = character(),
        hierarchies = list(),
        levels = list(),
        data = NULL,
        initialize = function(x, id.vars = key(x), hierarchies, .env){
            if(!missing(.env)){
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
            common.levels = Reduce(function(x, y){
                lapply(setNames(nm = unique(c(names(x), names(y)))),
                       function(nm) c(x[[nm]], y[[nm]]))
            }, hierarchies)
            self$levels =  lapply(setNames(nm = names(common.levels)), function(lvlk){
                as.level(x, id.vars = lvlk, properties = common.levels[[lvlk]])
            })
            dt = rbindlist(lapply(names(self$levels), function(lvlk) data.table(properties = c(lvlk, self$levels[[lvlk]]$properties))[, level := lvlk]))[, tail(.SD, 1L), properties]
            self$fields = dt$properties # TEST on error use: setNames(dt$properties, dt$level)
            # all.hierarchies.level.mappings
            granularity = unique(c(self$id.vars, all.hierarchies.level.keys))
            self$data = setkeyv(unique(x, by = granularity)[, .SD, .SDcols = granularity], self$id.vars)[]
            invisible(self)
        },
        dim = function(){
            if(!ncol(self$data)) return(0L)
            unname(unlist(self$data[, lapply(.SD, uniqueN), .SDcols = self$id.vars]))
        },
        print = function(){
            dimension.data.str = capture.output(str(self$data, give.attr = FALSE))
            cat(c("<dimension>", dimension.data.str), sep="\n")
            invisible(self)
        },
        # lvls.apply
        lvls.apply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE, lvls = names(self$levels)){
            FUN = match.fun(FUN)
            sapply(X = self$levels[lvls],
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        schema = function(){ # for each dimensions
            i = setNames(seq_along(self$levels), names(self$levels))
            levels_schema = rbindlist(lapply(i, function(i) self$levels[[i]]$schema()), idcol = "entity")
            dimension_data_schema = schema.data.table(self$data, empty = c("entity"))
            rbindlist(list(dimension_data_schema, levels_schema))
        },
        head = function(n = 6L){
            list(base = head(self$data, n), levels = lapply(self$levels, function(x) x$head(n = n)))
        },
        subset = function(i.meta, drop = TRUE){
            stopifnot(is.list(i.meta), is.logical(drop))
            filter.cols = names(i.meta)
            filter.lvls = sapply(self$levels, function(x) any(filter.cols %in% c(x$id.vars, x$properties)))
            filter.lvls = names(filter.lvls)[filter.lvls]
            level.fields = lapply(self$levels, function(x) c(x$id.vars, x$properties))
            filter.lvls.cols = lapply(level.fields, function(fields) filter.cols[filter.cols %in% fields])
            # if(!identical(sort(unique(unlist(filter.lvls.cols))), sort(filter.lvls))) browser()
            # stopifnot(identical(sort(unique(unlist(filter.lvls.cols))), sort(filter.lvls)))
            r = new.env()
            r$levels = lapply(setNames(nm = names(self$levels)), function(lvl){
                if(lvl %in% filter.lvls){
                    filter.cols.in.level = filter.lvls.cols[[lvl]]
                    i.meta.lvl = i.meta[filter.cols.in.level]
                    i.lvl = build.each.i(i.meta.lvl)
                    self$levels[[lvl]]$subset(i.lvl, drop = drop)
                } else self$levels[[lvl]]
            })
            # dimension base
            r$data = if(length(filter.lvls)) NULL else self$data
            r$id.vars = key(self$data)
            r$hierarchies = self$hierarchies
            r$fields = unname(self$fields)
            as.dimension(r)
        },
        base = function(x = self$data){
            level.keys = unique(unlist(lapply(self$hierarchies, names)))
            base.grain = unique(c(self$id.vars, level.keys))
            if(!length(x)) x = setDT(as.list(setNames(seq_along(base.grain), base.grain)))[0L]
            self$data = setkeyv(unique(x, by = base.grain)[, .SD, .SDcols = base.grain], self$id.vars)[]
            invisible(self)
        },
        index = function(.log = getOption("datacube.log")){
            NULL
        }
    )
)

#' @title Test if dimension class
#' @param x object to tests.
is.dimension = function(x) inherits(x, "dimension")
