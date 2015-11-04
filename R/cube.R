subarg = function(x){
    if(identical(x, substitute())) x = quote(list()) # empty args: x[,,"asd"] to x[.(),.(),"asd"]
    else if(is.call(x) && x[[1L]]==as.symbol(".")) x[[1L]] = quote(list) # decode x[.(y)] to x[list(y)]
    x
}

selfNames = function(x) setNames(x, x)

#' @title OLAP cube class
#' @docType class
#' @format An R6 class object.
#' @name cube
#' @details Generates cube class objects.
#' @aliases data.cube
cube = R6Class(
    classname = "cube",
    public = list(
        db = NULL,
        dims = character(),
        fact = character(),
        keys = character(),
        measures = character(),
        nr = integer(),
        nc = integer(),
        mb = numeric(),
        dim = integer(),
        dimcolnames = list(),
        initialize = function(fact, dims, aggregate.fun, db, ...){
            stopifnot(is.list(fact), length(fact)==1L)
            if(!missing(db)) stopifnot(is.environment(db)) else db = new.env(parent = topenv())
            self$db = db
            self$dims = names(dims)
            self$fact = names(fact)
            keycols = lapply(dims, function(x){
                if(!haskey(x)) setkeyv(x, names(x)[1L])
                key(x)
            })
            # - [x] check: all dimensions keys must be of length 1L
            if(!all(single_key <- sapply(keycols, length)==1L)) stop(sprintf("Dimension tables must be keyed by single columns, preferably surrogate keys. This is not true for: %s.", paste(self$dims[!single_key], collapse=", ")))
            self$keys = unlist(keycols)
            fact_col_names = names(fact[[self$fact]])
            self$measures = fact_col_names[!fact_col_names %in% self$keys]
            # - [x] check: keys to dimensions must exists in fact table
            if(!all(missing_key_col <- self$keys %in% fact_col_names)) stop(sprintf("Dimension key columns do not exists in fact table: %s.", paste(self$keys[missing_key_col], collapse=", ")))
            # - [x] check: fact table is already sub-aggregated to all dimensions
            if(!is.unique.data.table(fact[[self$fact]])){
                if(missing(aggregate.fun)) stop(sprintf("Fact table is not sub-aggregated and the `aggregate.fun` argument is missing. Sub-aggregated your fact table or provide aggregate function."))
                if(!is.function(aggregate.fun)) stop(sprintf("Fact table is not sub-aggregated and the `aggregate.fun` argument is not a function. Sub-aggregated your fact table or provide aggregate function."))
                # - [x] sub-aggregate fact table
                fact[[self$fact]] = fact[[self$fact]][, lapply(.SD, aggregate.fun, ...), c(self$keys)]
            }
            self$db[[self$fact]] = fact[[self$fact]]
            setkeyv(self$db[[self$fact]], unname(self$keys))
            # dimensions
            lapply(self$dims, function(dim){
                if(is.null(dims[[dim]])){
                    if(missing(db)) stop("Dimensions can be NULL only if they exists in db environment already which has not been provided as `db` argument.")
                    # - [x] skip loading dimension if NULL provided as dimension
                    if(!is.data.table(self$db[[dim]])) stop(sprintf("Dimensions can be NULL only if they exists in db environment already. That is not true for '%s' dimension.", dim))
                    # - [x] validate key column name and class match
                    if(!(dim_key <- key(self$db[[dim]])) %in% self$keys) stop(sprintf("You are trying to reuse dimension %s which key column does not exists in fact table.", dim))
                    if(!identical(class(self$db[[dim]][[dim_key]]), class(self$db[[self$fact]][[dim_key]]))) stop(sprintf("You are trying to reuse dimension %s which key column class is not identical to same column in fact table.", dim))
                } else {
                    if(!is.unique.data.table(dims[[dim]])) stop(sprintf("Provided dimension %s has key which is not unique. Use your uid as first column and setkey on it.", dim))
                    self$db[[dim]] = dims[[dim]]
                }
                invisible(TRUE)
            })
            self$nr = sapply(c(self$fact, self$dims), function(tbl) nrow(self$db[[tbl]]))
            self$nc = sapply(c(self$fact, self$dims), function(tbl) ncol(self$db[[tbl]]))
            self$mb = sapply(c(self$fact, self$dims), function(tbl) as.numeric(object.size(self$db[[tbl]]))/(1024^2))
            self$dim = self$nr[self$dims]
            self$dimcolnames = lapply(selfNames(self$dims), function(dim) names(self$db[[dim]]))
            invisible(self)
        },
        print = function(){
            prnt = character()
            prnt["head"] = "<cube>"
            prnt["tbls"] = sprintf("fact:\n  %s %s rows (%.2f MB)\ndims:\n%s",self$fact, self$nr[self$fact], self$mb[[self$fact]], paste(paste(" ",self$dims, self$nr[self$dims], "rows",sprintf("(%.2f MB)", self$mb[self$dims])), collapse="\n"))
            prnt["size"] = sprintf("total size: %.2f MB", sum(self$mb))
            cat(prnt, sep="\n")
        },
        join = function(x, dim, cols){
            x[self$db[[dim]],
              (cols) := mget(paste0("i.", cols)),
              on = c(key(self$db[[dim]]))]
        },
        extract = function(..., .dots){
            # - [x] catch dots, preprocess, evaluate
            if(missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            browser()
            args = lapply(lapply(.dots, subarg), eval)
            stopifnot(length(args) <= length(self$dims))
            names(args) = self$dims[seq_along(args)]
            completed = lapply(self$keys, I)
            r = NULL
            # - [x] check if all cols exists in dims, filters having NULL or vector are not checked as it refers to no filter and fact-filter
            cols_missing = sapply(names(args), function(arg) if(is.null(args[[arg]])) FALSE else if(!is.list(args[[arg]])) FALSE else !all(names(args[[arg]]) %in% names(self$db[[arg]])))
            if(any(cols_missing)) stop(sprintf("Field used in subset does not exists in dimensions %s.", paste(names(cols_missing)[cols_missing], collapse=", ")))
            # - [x] categorize filters into fact-filters and dimension-filters
            fact_filter = sapply(args, function(x) !is.list(x) && length(x))
            dim_filter = sapply(args, function(x) is.list(x))
            # - [x] check if binary search possible, only leading fact filters
            if(isTRUE(fact_filter[[1L]])){
                # data.table#1419 workaround, fixed in 1.9.7
                nm = names(fact_filter)
                binsearch_cols = rleid(fact_filter)==1L
                names(binsearch_cols) = nm
                names(fact_filter) = nm
                # workaround end
                # - [x] do binary search for key-leading cols for fact-filters
                r = self$db[[self$fact]][i = do.call(CJ, args[binsearch_cols]), nomatch = 0L]
                completed_binsearch = names(binsearch_cols)[binsearch_cols]
            } else completed_binsearch = character()
            # - [x] fact-filter after the gap as vector scans
            if(any(!(ff <- names(fact_filter)[fact_filter]) %in% completed_binsearch)){
                for(fff in ff[!ff %in% completed_binsearch]){
                    qi = as.call(list(quote(`%in%`), as.name(self$keys[[fff]]), args[[fff]]))
                    r = if(is.null(r)) self$db[[self$fact]][eval(qi)] else r[eval(qi)]
                }
            }
            # - [x] dimension-filters just before the join
            if(any(dim_filter)){
                # - [x] for each dimension
                for(dim in names(dim_filter)[dim_filter]){
                    joincol = key(self$db[[dim]])
                    if(identical(args[[dim]],list(NULL))){ # lookup all columns from dim
                        completed[[dim]] = unique(c(joincol, names(self$db[[dim]])))
                        qdim = quote(self$db[[dim]])
                        qfact = if(is.null(r)) quote(self$db[[self$fact]]) else quote(r)
                    } else {
                        dim_attrs = names(args[[dim]])
                        if(length(unique(dim_attrs))!=length(args[[dim]])) stop(sprintf("Dimensions hierarchy attributes has to be uniquely named `.(a1=..., a2=...)`, only lookup all dimension attributes does not require names and can be used as `.(NULL)`."))
                        dim_attrs_filter = sapply(dim_attrs, function(attr) !is.null(args[[dim]][[attr]]))
                        # - [x] handle filters of multiple attributes in single dimension
                        dim_attrs_filter_cols = names(dim_attrs_filter)[dim_attrs_filter]
                        dim_attrs_filter_calls = lapply(selfNames(dim_attrs_filter_cols), function(attr) as.call(list(quote(`%in%`), as.name(attr), args[[dim]][[attr]])))
                        qi = Reduce(function(a,b) bquote(.(a) & .(b)), dim_attrs_filter_calls)
                        # use data.table index on dimensions while filter - this has to be done on client side when creating cube
                        # prepare for join
                        completed[[dim]] = unique(c(joincol, dim_attrs))
                        qj = as.call(lapply(c("list", unique(c(joincol, dim_attrs))), as.symbol))
                        qdim = if(is.null(qi)) quote(self$db[[dim]][, eval(qj)]) else quote(self$db[[dim]][eval(qi), eval(qj)])
                        qfact = if(is.null(r)) quote(self$db[[self$fact]]) else quote(r)
                    }
                    r = eval(qdim)[eval(qfact), on = c(joincol), nomatch = 0L]
                }
            }
            # - [x] reorder returned columns to match dimensions sequence in cube object
            setcolorder(r, c(unlist(completed), self$measures))
            setkeyv(r, unname(sapply(completed, `[`, 1L)))
            return(r)
        },
        subset = function(..., .dots){
            # - [x] catch dots, preprocess, evaluate
            if(missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            args = lapply(lapply(.dots, subarg), eval)
            stopifnot(length(args) <= length(self$dims))
            # - [x] stop if provided dims subset which doesn't match to dims
            if(!is.null(names(args))){
                subset_dimnames = names(args)[names(args) != ""]
                model_dimnanmes = self$dims[seq_along(args)][names(args) != ""]
                if(any(subset_dimnames!=model_dimnanmes)) stop(sprintf("Provided dimension names doesn't match to model dimension sequence. Instead of %s you should provide %s.",
                                                                       paste(subset_dimnames[subset_dimnames!=model_dimnanmes], collapse=", "),
                                                                       paste(model_dimnanmes[subset_dimnames!=model_dimnanmes], collapse=", ")))
            }
            names(args) = self$dims[seq_along(args)]
            skipped_dims = length(self$dims) - length(args)
            if(skipped_dims > 0L){
                args[self$dims[length(args)+1:skipped_dims]] = lapply(1:skipped_dims, function(x) list())
            }
            transarg = function(dim){
                x = args[[dim]]
                if(is.null(x)) return(x) # null args: x[NULL,NULL,"asd"]
                else if(is.list(x) && length(x)==1L && is.null(x[[1L]])) return(x) # x[.(NULL)]
                else if(!is.list(x)) return(setNames(list(x), self$keys[[dim]])) # x[,,"asd"] to x[,,.(keycol="asd")]
                else if(is.list(x) & length(x)==0L) return(setNames(vector(mode = "list", self$nc[[dim]]), self$dimcolnames[[dim]])) # x[.()] to x[.(col1=NULL,col2=NULL)]
                else if(is.list(x) & length(x)){
                    # - [x] check if hierarchy attributes uniquely named
                    stopifnot(!is.null(names(x)), uniqueN(names(x))==length(names(x)))
                    return(x)
                }
            }
            args = lapply(selfNames(names(args)), transarg)
            # - [x] check if all cols exists in dims, filters having NULL or vector are not checked as it refers to no filter and fact-filter
            cols_missing = sapply(names(args), function(dim) if(!is.list(args[[dim]])) FALSE else !all(names(args[[dim]]) %in% self$dimcolnames[[dim]]))
            if(any(cols_missing)) stop(sprintf("Field used in subset does not exists in dimensions %s.", paste(names(cols_missing)[cols_missing], collapse=", ")))
            # - [x] define dimensions processing: select columns from dims and apply filters
            select = lapply(selfNames(names(args)), function(dim) unique(c(self$keys[[dim]], if(!is.null(args[[dim]])) names(args[[dim]]))))
            filter = lapply(selfNames(names(args)), function(dim){
                if(is.null(args[[dim]])) 0L
                else if(!is.null(args[[dim]])){
                    build_call = function(attr) if(!is.null(args[[dim]][[attr]])) as.call(list(quote(`%in%`), as.name(attr), args[[dim]][[attr]]))
                    Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(args[[dim]]), build_call))
                }
            })
            # - [x] iterate over dimensions - cleaner than mapply
            dim.env = new.env()
            for(dim in self$dims){
                if(getOption("datacube.verbose", FALSE)) cat(sprintf("data.cube: Filtering dimension '%s'.\n", dim))
                # dimensions are not automatically dropped at that point
                qi = filter[[dim]]
                qj = as.call(lapply(c("list", select[[dim]]), as.symbol)) # .SD made locked result so building `j` manually
                dim.env[[dim]] = if(is.null(filter[[dim]])) self$db[[dim]][, eval(qj)] else self$db[[dim]][eval(qi), eval(qj)]
                setkeyv(dim.env[[dim]], self$keys[[dim]])
            }
            # - [x] check if binary search possible, only leading fact filters
            fact_filter = !sapply(filter, is.null)
            r = NULL
            if(isTRUE(fact_filter[[1L]])){
                # data.table#1419 workaround, fixed in 1.9.7
                nm = names(fact_filter)
                binarysearch_dims = rleid(fact_filter)==1L
                names(binarysearch_dims) = nm
                names(fact_filter) = nm
                # workaround end
                # - [x] do binary search for key-leading cols for fact-filters
                true_binarysearch_dims = names(binarysearch_dims)[binarysearch_dims]
                binsearch_vals = lapply(setNames(true_binarysearch_dims, self$keys[true_binarysearch_dims]), function(dim) dim.env[[dim]][[1L]])
                r = self$db[[self$fact]][i = do.call(CJ, binsearch_vals), nomatch = 0L]
            } else binarysearch_dims = setNames(rep(FALSE, length(fact_filter)), names(fact_filter))
            # - [x] fact-filter after the gap as vector scans
            if(any(!(ff <- names(fact_filter)[fact_filter]) %in% binarysearch_dims[binarysearch_dims])){
                filter_after_gap = ff[!ff %in% binarysearch_dims[binarysearch_dims]]
                dim.id = lapply(setNames(filter_after_gap, self$keys[filter_after_gap]), function(dim) dim.env[[dim]][[1L]])
                qi = Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(dim.id), function(col) as.call(list(quote(`%in%`), as.name(col), dim.id[[col]]))))
                r = if(is.null(r)) self$db[[self$fact]][eval(qi)] else r[eval(qi)]
            }
            # - [x] return cube with all dimensions filtered
            as.cube(x = list(), fact = setNames(list(if(is.null(r)) copy(self$db[[self$fact]]) else r), self$fact), dims = as.list(dim.env)[self$dims])
        },
        drop = function(drop=1L){
            # Direct access to cube object method by `$drop()` should not be used on cubes that shares dimensions, still you can use drop on `[.cube` subset safety
            if(drop >= 1L){
                # - [x] drop dimensions where cardinality = 1
                cardinality =  self$db[[self$fact]][, lapply(.SD, uniqueN), .SDcols = c(self$keys)]
                dims_to_drop = sapply(cardinality, `==`, 1L)
                self$dims = self$dims[!self$dims %in% names(dims_to_drop)[dims_to_drop]]
                rm(envir = self$db, list = names(dims_to_drop)[dims_to_drop])
                self$keys = sapply(self$dims, function(dim) key(self$db[[dim]]))
                self$nr = self$nr[c(self$fact, self$dims)]
                self$nc = self$nc[c(self$fact, self$dims)]
                self$mb = self$mb[c(self$fact, self$dims)]
            }
            if(drop >= 2L){
                # - [x] drop dimension members not present in fact table
                dim_keys_to_drop = sapply(self$dims, function(dim) self$nr[[dim]] > cardinality[[dim]])
                if(any(dim_keys_to_drop)){
                    sapply(names(dim_keys_to_drop)[dim_keys_to_drop],
                           function(dim){
                               self$db[[dim]] = self$db[[dim]][.(unique(self$db[[self$fact]][[self$keys[[dim]]]]))]
                               setkeyv(self$db[[dim]], self$keys[[dim]])
                               self$nr[[dim]] = nrow(self$db[[dim]])
                               self$mb[[dim]] = as.numeric(object.size(self$db[[dim]]))/(1024^2)
                               TRUE
                           })
                }
            }
            if(drop >= 1L){
                self$dim = self$nr[self$dims]
                self$dimcolnames = self$dimcolnames[self$dims]
            }
            self
        },
        apply = function(MARGIN, FUN, ...){
            stopifnot(is.character(MARGIN) | is.list(MARGIN), is.function(FUN))

        },
        query = function(){
            # - [ ] prepare order of join
            # highest self$nr with filter first
            browser()
            self$nr[dims[dims_filter]]
            # - [ ] loop over dimensions to lookup cols and apply filters
            filter_dim <- logical()
            r = copy(self$db[[self$fact]])
            for(dim in names(dims)){ # dim = dims[1]
                # - [ ] join
                self$join(r, dim, cols)
                # - [ ] filter fact
                if(filter_dim[dim]){
                    # - [ ] filter based on axes filter

                    # - [ ] filter based on where filter
                    #r = r[1L]
                }
                # - [ ] sub-aggregate to reduce nrow in case if we lookup higher (non-granular) level attribute from dimension hierarchy
                # TO DO
            }
            browser()
            # - [ ] aggregate
            self$apply()
            Call = self$qcall()
            eval(match.call((getFromNamespace("[.data.table", "data.table")), Call), envir = self$db)
        },
        denormalize = function(){
            self$subset(.dots = lapply(self$dims, function(x) list()))
        },
        dimspace = function(dims, drop=TRUE){
            if(missing(dims)) dims = self$dims
            lapply(selfNames(dims), function(dim){
                if(drop) self$db[[dim]][[1L]] else if(!drop) self$db[[dim]][, 1L, with=FALSE] else stop("dimspace method allows `drop` arg to be TRUE or FALSE.")
            })
        }
    )
)

# as.cube -----------------------------------------------------------------

#' @title Cast to OLAP cube
#' @param x R object
#' @param \dots arguments passed to methods
#' @description Converts arguments to *cube* class. Supports *list*, *array* (no hierarchies), *data.table*.
#' @return *cube* class object.
as.cube = function(x, ...){
    UseMethod("as.cube")
}

as.cube.default = function(x, ...){
    as.cube(as.array(x, ...))
}

as.cube.array = function(x, fact = "fact", dims, na.rm=TRUE, ...){
    stopifnot(is.character(fact), length(fact)==1L)
    dt = as.data.table(x, na.rm=na.rm)
    dim_cols = names(dt)[-length(dt)]
    dim_cols = if(missing(dims)){
        selfNames(dim_cols)
    } else {
        stopifnot(is.character(dims), length(dims)==length(dim_cols))
        setNames(dim_cols, dims)
    }
    cube$new(fact = setNames(list(dt), fact),
             dims = lapply(dim_cols, function(dim_col) setDT(setNames(list(unique(dt[[dim_col]])), dim_col), key = dim_col)))
}

as.cube.list = function(x, aggregate.fun, ..., fact, dims){
    if(!missing(fact) & !missing(dims)){
        stopifnot(is.list(fact), length(fact)==1L, is.data.table(fact[[1L]]), is.list(dims), all(sapply(dims, is.data.table)))
        cube$new(fact = fact, 
                 dims = dims, 
                 aggregate.fun = aggregate.fun,
                 ... = ...)
    } else {
        stopifnot(is.list(x), all(c("fact","dims") %in% names(x)))
        cube$new(fact = x$fact, 
                 dims = x$dims, 
                 aggregate.fun = aggregate.fun,
                 ... = ...)
    }
}

as.cube.data.table = function(x, fact = "fact", dims, aggregate.fun = sum, ...){
    stopifnot(is.data.table(x), is.character(fact), is.list(dims), as.logical(length(dims)), length(names(dims))==length(unique(names(dims))), all(sapply(dims, is.character)), all(sapply(dims, length)), is.function(aggregate.fun))
    fact_cols = c(sapply(dims, `[`, 1L), names(x)[!names(x) %in% unlist(dims)])
    cube$new(fact = setNames(list(x[, fact_cols, with=FALSE]), fact),
             dims = lapply(dims, function(cols) setkeyv(x[, unique(.SD), .SDcols = c(cols)], cols[1L])),
             aggregate.fun = aggregate.fun,
             ... = ...)
}

# as.others.cube ----------------------------------------------------------

as.array.cube = function(x, measure, ...){
    if(missing(measure)) measure = x$measures[1L]
    as.array(x = x$db[[x$fact]], dimnames = x$dimspace(), measure = measure)
}

as.data.table.cube = function(x){
    x$denormalize()
}

as.list.cube = function(x, fact = "fact", ...){
    list(fact = setNames(list(x$db[[x$fact]]), fact), dims = lapply(selfNames(x$dims), function(dim) x$db[[dim]]))
}

# capply ------------------------------------------------------------------

#' @title apply over cube dimensions
#' @param x cube object
#' @param MARGIN character or list
#' @param FUN function
#' @param ... arguments passed to *FUN*
capply = aggregate.cube = function(x, MARGIN, FUN, ...){
    stopifnot(inherits(x, "cube"))
    x$apply(MARGIN, FUN, ...)
}

# `*.cube` ----------------------------------------------------------------

#' @title subset cube
#' @param x cube object
#' @param ... values to subset on corresponding dimensions, when wrapping in list it will refer to dimension hierarchy
#' @param drop logical, default may switch to TRUE while implemented.
#' @return When *drop* arg is TRUE then 1-cardinality dimensions will be removed, also dimension members not linked to fact will be removed.
"[.cube" = function(x, ..., drop = FALSE){
    if(!is.logical(drop)) stop("`drop` argument to cube subset must be logical. If argument name conflicts with your dimension name then provide it without name, elements in ... are matched by positions - as in array method - not names.")
    r = x$subset(.dots = match.call(expand.dots = FALSE)$`...`)
    if(isTRUE(drop)) r$drop() else r
}

"[[.cube" = function(x, ...){
    r = x$extract(.dots = match.call(expand.dots = FALSE)$`...`)
    r
}

dim.cube = function(x){
    x$dim
}