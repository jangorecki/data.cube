# stats collector for data.table and big.data.table
schema.big.data.table = function(x, empty){
    if(requireNamespace("big.data.table", quietly = TRUE)){
        rscl = attr(x, "rscl")
        dm = dim(x)
        mb = sum(big.data.table::rscl.eval(rscl, as.numeric(object.size(x))/(1024^2), simplify = TRUE))
        adr = NA_character_
        ky = big.data.table::rscl.eval(rscl[1L], if(haskey(x)) paste(key(x), collapse=", ") else NA_character_, simplify = TRUE)
        dt = data.table(nrow = dm[1L], ncol = dm[2L], mb = mb, address = adr, sorted = ky)
        nn = copy(names(dt))
        if(!missing(empty)) setcolorder(dt[, c(empty) := NA], unique(c(empty, nn)))
        dt
    } else stop("schema.big.data.table can be only used with `big.data.table` package which is not installed.")
}
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
        id.vars = character(),
        properties = character(),
        data = NULL,
        initialize = function(x, id.vars = key(x), properties){
            stopifnot(is.data.table(x), is.character(id.vars), id.vars %in% names(x), is.character(properties), properties %in% names(x))
            self$id.vars = id.vars
            self$properties = unique(properties)
            self$data = setkeyv(unique(x, by = self$id.vars)[, j = .SD, .SDcols = unique(c(self$id.vars, self$properties))], self$id.vars)[]
            invisible(self)
        },
        print = function(){
            lvl.data.str = capture.output(str(self$data, give.attr = FALSE))
            cat(c("<level>", lvl.data.str), sep="\n")
            invisible(self)
        },
        schema = function(){
            schema.data.table(self$data)
        },
        head = function(n = 6L){
            head(self$data, n)
        },
        subset = function(i, drop = TRUE){
            level$new(x = self$data[eval(i)], id.vars = key(self$data), properties = self$properties)
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
        initialize = function(levels){
            stopifnot(is.list(levels), as.logical(length(levels)))
            self$levels = levels
            invisible(self)
        },
        print = function(){
            hierarchy.str = capture.output(str(self$levels, give.attr = FALSE))
            cat(c("<hierarchy>", hierarchy.str), sep="\n")
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
            self$hierarchies = lapply(hierarchies, function(levels) hierarchy$new(levels = levels))
            # combine by levels
            common.levels = Reduce(function(x, y){
                lapply(setNames(nm = unique(c(names(x), names(y)))),
                       function(nm) c(x[[nm]], y[[nm]]))
            }, hierarchies)
            self$levels =  lapply(setNames(nm = names(common.levels)), function(lvlk){
                level$new(x, id.vars = lvlk, properties = common.levels[[lvlk]])
            })
            dt = rbindlist(lapply(names(self$levels), function(lvlk) data.table(properties = c(lvlk, self$levels[[lvlk]]$properties))[, level := lvlk]))[, tail(.SD, 1L), properties]
            self$fields = dt$properties # TEST on error use: setNames(dt$properties, dt$level)
            # all.hierarchies.level.mappings
            granularity = unique(c(self$id.vars, all.hierarchies.level.keys))
            self$data = setkeyv(unique(x, by = granularity)[, .SD, .SDcols = granularity], self$id.vars)[]
            invisible(self)
        },
        dim = function(){
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
        initialize = function(x, id.vars = character(), measure.vars = character(), fun.aggregate = "sum", ..., measures, .env){
            if(!missing(.env)){
                # skip heavy processing for env argument
                self$local = .env$local
                self$id.vars = .env$id.vars
                self$measure.vars = .env$measure.vars
                self$measures = .env$measures
                self$data = .env$data
                return(invisible(self))
            }
            stopifnot(is.character(id.vars), is.character(measure.vars), is.character(fun.aggregate))
            self$id.vars = id.vars
            # - [x] `fact$new` creates measures, or use provided in `measures` argument
            if(!missing(measures)){
                self$measures = measures
                self$measure.vars = names(self$measures)
            } else {
                if(!length(measure.vars)) stop("You need to provide at least one measure column name")#measure.vars = setdiff(names(x), self$id.vars)
                self$measure.vars = measure.vars
                self$measures = lapply(setNames(nm = self$measure.vars), function(var) measure$new(var, fun.aggregate = fun.aggregate, ... = ...))
            }
            #if(!all(sapply(self$measures, inherits, "measure"))) 
                # browser()
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
        dim = function(){
            unname(unlist(self$data[, lapply(.SD, uniqueN), .SDcols = self$id.vars]))
        },
        print = function(){
            fact.data.str = capture.output(str(self$data, give.attr = FALSE))
            cat(c("<fact>", fact.data.str), sep="\n")
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
            if(!self$local) schema.big.data.table(self$data, empty = c("entity")) else schema.data.table(self$data, empty = c("entity"))
        },
        head = function(n = 6L){
            head(self$data, n)
        },
        subset = function(){
            NULL
        },
        index = function(.log = getOption("datacube.log")){
            NULL
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
        id.vars = character(),
        dimensions = list(),
        initialize = function(fact, dimensions, .env){
            stopifnot(is.fact(fact), sapply(dimensions, is.dimension))
            self$dimensions = dimensions
            self$id.vars = lapply(self$dimensions, `[[`, "id.vars")
            self$fact = fact
            invisible(self)
        },
        dim = function(){
            # fd = self$fact$dim() # heavy
            unname(sapply(self$dimensions, function(x) nrow(x$data)))
        },
        print = function(){
            dict = self$schema()
            prnt = character()
            prnt["header"] = "<data.cube>"
            #prnt["distributed"] = 
            n.measures = length(self$fact$measure.vars)
            prnt["fact"] = dict[type=="fact",
                                sprintf("fact%s:\n  %s rows x %s dimensions x %s measures (%.2f MB)", 
                                        if(!self$fact$local) sprintf(" (distributed on %s nodes)", length(attr(self$fact$data, "rscl"))) else "",
                                        nrow, ncol - n.measures, n.measures, mb)]
            if(length(self$dimensions)){
                dt = dict[type=="dimension", .(nrow = nrow[is.na(entity)], ncol = ncol[is.na(entity)], mb = sum(mb, na.rm = TRUE)), .(name)]
                prnt["dims"] = paste0("dimensions:\n", paste(dt[, sprintf("  %s : %s entities x %s levels (%.2f MB)", name, nrow, ncol, mb)], collapse="\n"))
            }
            prnt["size"] = sprintf("total size: %.2f MB", dict[,sum(mb)])
            cat(prnt, sep = "\n")
            invisible(self)
        },
        # dims.apply
        dims.apply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE, dims = names(self$dimensions)){
            FUN = match.fun(FUN)
            sapply(X = self$dimensions[dims],#lapply(setNames(nm = dims), function(dd) self$dimensions[[dd]]),
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        # fact.apply
        fact.apply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE){
            FUN = match.fun(FUN)
            sapply(X = list(self$fact),
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        denormalize = function(dims = names(self$dimensions), na.fill = FALSE){
            n.dims = length(dims)
            all_cols = lapply(self$dims.apply(function(x) lapply(x$levels, names.level), dims = dims), `[[`, 1L)
            key_cols = sapply(all_cols, `[[`, 1L)
            lkp_cols = lapply(all_cols, `[`, -1L)
            # that already solved later in `lookupv`
            #if(!is.unique(unlist(lkp_cols))) browser()#stop("Cannot lookup dimension attributes due to the column names duplicated between dimensions.")
            r = if(!na.fill | n.dims == 0L){
                copy(self$fact$data)
            } else {
                # `nomatch` to be extended after data.table#857 resolved
                self$fact$data[i = do.call(CJ, c(self$dims.apply(function(x) x$data[[1L]]), list(sorted = TRUE, unique = TRUE))), nomatch = NA, on = swap.on(key_cols)]
            }
            # lookup
            lookupv(dims = lapply(self$dimensions[dims], as.data.table.dimension), r)
            if(n.dims > 0L) setkeyv(r, unname(key_cols))[] else r[]
        },
        schema = function(){
            rbindlist(list(
                fact = rbindlist(list(fact = self$fact$schema()), idcol = "name"),
                dimension = rbindlist(lapply(self$dimensions, function(x) x$schema()), idcol = "name")
            ), idcol = "type")
        },
        head = function(n = 6L){
            list(fact = self$fact$head(n = n), dimensions = lapply(self$dimensions, function(x) x$head(n = n)))
        },
        parse.i = function(i){
            # handling various types of input to [.cube `...` argument and [[.cube `i` argument.
            parse.each.i = function(int, i, keys){
                # preprocessing of `...` arg of `[.cube` and `i` arg of `[[.cube`
                stopifnot(is.integer(int), is.pairlist(i), is.character(keys))
                x = if(int > length(i)) list() else i[[int]] # fill missing args: x[,] to x[,,.()] in case of 3 dimensions
                if(missing(x)) x = list() # empty args: x[,,"asd"] to x[.(),.(),"asd"]
                else if(is.null(x)) x = setNames(list(NULL), keys[[int]]) # null args: x[NULL,NULL,"asd"] to x[.(keycol=NULL),.(keycol=NULL),"asd"]
                if(is.call(x) && x[[1L]]==as.symbol(".")) x[[1L]] = quote(list) # decode x[.(y)] to x[list(y)]
                x = eval.parent(x) # x[,,var] to x[,,.(keycol=c("val1","val2"))], x[,,c("asd","asd2")] to x[,,.(keycol=c("asd","asd2"))]
                if(is.atomic(x)) x = setNames(list(x), keys[[int]]) # x[,,"asd"] to x[,,.(keycol="asd")]
                stopifnot(is.list(x))
                if(length(x)==1L && is.null(x[[1L]]) && is.null(names(x[1L]))) x = setNames(x, keys[[int]]) # x[.(NULL)] to x[.(keycol=NULL)]
                if(length(x)) stopifnot(length(unique(names(x)))==length(names(x))) # unique names
                x
            }
            keys = self$fact$id.vars
            i = lapply(setNames(seq_along(keys), names(keys)), parse.each.i, i, keys)
            # - [x] check if all cols exists in dims
            cols_missing = sapply(names(i), function(dimk) !all(names(i[[dimk]]) %in% self$dimensions[[dimk]]$fields))
            if(any(cols_missing)) stop(sprintf("Field used in query does not exists in dimensions: %s.", paste(names(cols_missing)[cols_missing], collapse=", ")))
            i
        },
        # [.data.cube
        subset = function(..., .dots, drop = TRUE){
            # - [x] catch dots, preprocess, evaluate
            if(missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            i.meta = self$parse.i(.dots)
            dims.filter = lapply(i.meta, build.each.i)
            # - [x]  no filters returns self
            fact_filter = !sapply(dims.filter, is.null)
            if(all(!fact_filter)){
                return(self)
            }
            # returned object
            r = new.env()
            r$id.vars = self$fact$id.vars
            # - [x] filter dimensions and levels
            dim.names = setNames(nm = names(self$dimensions))
            r$dimensions = lapply(dim.names, function(d){
                if(d %in% names(fact_filter)[fact_filter]) self$dimensions[[d]]$subset(i.meta = i.meta[[d]], drop = drop) else self$dimensions[[d]]
            })
            # - [x] NULL subset returns empty fact
            if(any(sapply(dims.filter, identical, 0L))){
                r$fact = as.fact(x = self$fact$data[0L], id.vars = self$fact$id.vars, measure.vars = self$fact$measure.vars, measures = self$fact$measures)
                return(as.data.cube(r))
            }
            
            browser()
            
            # dev
            
            # - [ ] get dimension base key values after dim filtering to subset fact table
            if(any(fact_filter)){
                lapply(dim.names, function(d){
                    if(d %in% names(fact_filter)[fact_filter]) self$dimensions[[d]]$base(self$fact$data) else self$dimensions[[d]]
                })
                # i.meta = list()
                # r$fact = self$fact$subset(i.dt, drop = drop)
            }
            
            # # - [x] check if binary search possible, only leading fact filters
            # if(isTRUE(fact_filter[[1L]])){
            #     fact_filter2 = copy(fact_filter) # data.table#1419 rleid workaround as `copy()`, fixed in 1.9.7
            #     binarysearch_dims = self$dims[rleid(fact_filter2)==1L]
            #     if(getOption("datacube.verbose", FALSE)) cat(sprintf("data.cube: filter facts using binary search on '%s'.\n", paste(binarysearch_dims, collapse=", ")))
            #     r$fact = self$fact$data[i = do.call(CJ, lapply(selfNames(binarysearch_dims), function(dim) r$dims[[dim]][[1L]])), nomatch = NA]
            # } else binarysearch_dims = character()
            # # - [x] fact-filter after the gap as vector scans
            # vectorscan_dims = setdiff(self$dims[fact_filter], binarysearch_dims)
            # if(length(vectorscan_dims)){
            #     if(getOption("datacube.verbose", FALSE)) cat(sprintf("data.cube: filter facts using vector scan on '%s'.\n", paste(vectorscan_dims, collapse=", ")))
            #     dims.keys = lapply(setNames(vectorscan_dims, keys[vectorscan_dims]), function(dim) r$dims[[dim]][[1L]])
            #     qi = Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(dims.keys), function(col) as.call(list(quote(`%in%`), as.name(col), dims.keys[[col]]))))
            #     r$fact[[self$fact]] = if(!length(r$fact[[self$fact]])) self$env$fact[[self$fact]][eval(qi)] else r$fact[[self$fact]][eval(qi)]
            # }
            # - [x] return cube with all dimensions filtered and fact filtered
            return(as.data.cube(r))
        },
        index = function(){
            optional.logR = function(x, .log = getOption("datacube.log")){
                if(isTRUE(.log)) eval(substitute(logR(x), list(x = substitute(x)))) else x
            }
            list(self$fact$index(),
                 lapply(self$dimensions, function(x) optional.logR(x$index())))
        }
    )
)

# dim.* ----

dim.dimension = function(x){
    stopifnot(is.dimension(x))
    x$dim()
}

dim.fact = function(x){
    stopifnot(is.fact(x))
    x$dim()
}

dim.data.cube = function(x){
    stopifnot(is.data.cube(x))
    x$dim()
}

# names.* ----

names.level = function(x) names(x$data)
names.dimension = function(x) names(x$data)
names.fact = function(x) names(x$data)
names.data.cube = function(x) names(x$fact)

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

as.level = function(x, ...){
    UseMethod("as.level")
}

as.level.data.table = function(x, id.vars = key(x), properties, ...){
    stopifnot(is.character(properties), is.character(id.vars), length(id.vars) > 0L)
    level$new(x = x, id.vars = id.vars, properties = properties)
}

#' @title Build dimension
#' @param x data.table or object with a \emph{as.data.table} method, build dimension based on that dataset.
#' @param \dots arguments passed to methods.
#' @param id.vars character scalar of dimension primary key.
#' @param hierarchies list of hierarchies of levels and its attributes.
#' @return dimension class object.
as.dimension = function(x, ...){
    UseMethod("as.dimension")
}

#' @rdname as.dimension
#' @method as.dimension default
as.dimension.default = function(x, id.vars, hierarchies, ...){
    stopifnot(is.list(hierarchies), is.character(id.vars), length(id.vars) > 0L)
    as.dimension.data.table(as.data.table(x), id.vars = id.vars, hierarchies = hierarchies)
}

#' @rdname as.dimension
#' @method as.dimension data.table
as.dimension.data.table = function(x, id.vars = key(x), hierarchies = list(setNames(rep(list(character(0)), length(id.vars)), id.vars)), ...){
    stopifnot(is.list(hierarchies), is.character(id.vars), length(id.vars) > 0L)
    dimension$new(x = x, id.vars = id.vars, hierarchies = hierarchies)
}

as.dimension.environment = function(x, ...){
    dimension$new(.env = x)
}

#' @title Form measure
#' @param x character scalar column name of a measure.
#' @param label character scalar, a label for a measure.
#' @param fun.aggregate character scalar function name.
#' @param \dots arguments passed to \emph{fun.aggregate}.
#' @param fun.format function to be used for formatting of a measure, such a currency.
#' @return measure class object.
as.measure = function(x, label = character(), fun.aggregate = "sum", ..., fun.format = function(x) x){
    measure$new(x, label = label, fun.aggregate = fun.aggregate, ... = ..., fun.format = fun.format)
}

#' @title Build fact
#' @param x data.table build dimension based on that dataset.
#' @param id.vars character vector of all dimension's foreign keys.
#' @param measure.vars character vector, column names of measures.
#' @param fun.aggregate default "sum".
#' @param \dots arguments to fun.aggregate.
#' @param measures list of measures class objects, useful if various measures needs to have different `fun.aggregate`.
#' @return fact class object.
as.fact = function(x, ...){
    UseMethod("as.fact")
}

#' @rdname as.fact
#' @method as.fact default
as.fact.default = function(x, id.vars = character(), measure.vars = character(), fun.aggregate = "sum", ..., measures){
    as.fact.data.table(as.data.table(x, ...))
}

#' @rdname as.fact
#' @method as.fact data.table
as.fact.data.table = function(x, id.vars = key(x), measure.vars = setdiff(names(x), id.vars), fun.aggregate = "sum", ..., measures){
    fact$new(x, id.vars = id.vars, measure.vars = measure.vars, fun.aggregate = fun.aggregate, ... = ..., measures = measures)
}

#' @title Build cube
#' @param x R object.
#' @param \dots arguments passed to methods.
#' @param dimensions list of dimension class objects.
#' @param na.rm logical, default TRUE, when FALSE the cube would store cross product of all dimension grain keys!
#' @return data.cube class object.
as.data.cube = function(x, ...){
    UseMethod("as.data.cube")
}

#' @rdname as.data.cube
#' @method as.data.cube default
as.data.cube.default = function(x, ...){
    as.data.cube.array(as.array(x, ...))
}

#' @rdname as.data.cube
#' @method as.data.cube array
as.data.cube.array = function(x, na.rm = TRUE, ...){
    ar.dimnames = dimnames(x)
    dt = as.data.table(x, na.rm = na.rm)
    ff = as.fact(dt, id.vars = key(dt), measure.vars = "value")
    dd = lapply(setNames(nm = names(ar.dimnames)), function(nm){
        as.dimension(setNames(list(ar.dimnames[[nm]]), nm),
                     id.vars = nm,
                     hierarchies = list(setNames(list(character(0)), nm)))
        
    })
    as.data.cube.fact(ff, dd)
}

#' @rdname as.data.cube
#' @method as.data.cube fact
as.data.cube.fact = function(x, dimensions, ...){
    stopifnot(
        is.list(dimensions),
        sapply(dimensions, is.dimension),
        is.fact(x)
    )
    data.cube$new(x, dimensions)
}

as.data.cube.environment = function(x, ...){
    data.cube$new(.env = x)
}

as.data.cube.cube = function(x, ...){
    ff = x$fapply(as.fact.data.table)[[x$fact]]
    dd = x$dapply(as.dimension.data.table)
    as.data.cube.fact(ff, dd)
}

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
