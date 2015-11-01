cube = R6Class(
    classname = "cube",
    public = list(
        db = NULL,
        dims = character(),
        fact = character(),
        keys = character(),
        nr = integer(),
        nc = integer(),
        mb = numeric(),
        initialize = function(fact, dims, aggregate, db){
            stopifnot(is.list(fact), length(fact)==1L)
            if(!missing(db)) stopifnot(is.environment(db)) else db = new.env(parent = topenv())
            self$db = db
            self$dims = names(dims)
            self$fact = names(fact)
            keycols = lapply(setNames(dims, self$dims), key)
            # - [ ] check: all dimensions keys must be of length 1L
            if(!all(single_key <- sapply(keycols, length)==1L)) stop(sprintf("Dimension tables must be keyed by single columns, preferably surrogate keys. This is not true for %s.", paste(self$dims[single_key], collapse=", ")))
            self$keys = unlist(keycols)
            # - [ ] check: keys to dimensions must exists in fact table
            if(!all(missing_key_col <- self$keys %in% names(fact[[self$fact]]))) stop(sprintf("Dimension key columns do not exists in fact table: %s.", paste(self$keys[missing_key_col], collapse=", ")))
            # - [ ] check: fact table is already sub-aggregated to all dimensions
            if(fact[[self$fact]][, .(count = .N), c(self$keys)][, any(count > 1L)]){
                if(missing(aggregate)) stop(sprintf("Fact table is not sub-aggregated and the `aggregate` argument is missing. Sub-aggregated your fact table or provide aggregate function."))
                if(!is.function(aggregate)) stop(sprintf("Fact table is not sub-aggregated and the `aggregate` argument is not a function. Sub-aggregated your fact table or provide aggregate function."))
                # - [x] sub-aggregate fact table
                fact[[self$fact]] = fact[[self$fact]][, lapply(.SD, aggregate), c(self$keys)]
            }
            self$db[[self$fact]] = fact[[self$fact]]
            setkeyv(self$db[[self$fact]], unname(self$keys))
            lapply(self$dims, function(dim){
                if(is.null(dims[[dim]])){
                    if(missing(db)) stop("Dimensions can be NULL only if they exists in db environment already which has not been provided as `db` argument.")
                    # - [x] skip loading dimension if NULL provided as dimension
                    if(!is.data.table(self$db[[dim]])) stop(sprintf("Dimensions can be NULL only if they exists in db environment already. That is not true for '%s' dimension.", dim))
                    # - [x] validate key column name and class match
                    if(!(dim_key <- key(self$db[[dim]])) %in% self$keys) stop(sprintf("You are trying to reuse dimension %s which key column does not exists in fact table.", dim))
                    if(!identical(class(self$db[[dim]][[dim_key]]), class(self$db[[self$fact]][[dim_key]]))) stop(sprintf("You are trying to reuse dimension %s which key column class is not identical to same column in fact table.", dim))
                } else {
                    self$db[[dim]] = dims[[dim]]
                }
                invisible(TRUE)
            })
            self$nr = sapply(c(self$fact, self$dims), function(tbl) nrow(self$db[[tbl]]))
            self$nc = sapply(c(self$fact, self$dims), function(tbl) ncol(self$db[[tbl]]))
            #if(any(self$nc < 2L)) stop("Each table must have at least 2 columns.")
            self$mb = sapply(c(self$fact, self$dims), function(tbl) as.numeric(object.size(self$db[[tbl]]))/1024/1024)
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
        subset = function(..., .dots){
            # - [ ] catch dots, preprocess, evaluate
            if(missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            subarg = function(x){
                if(identical(x, substitute())) return(NULL) # empty args: x[,,"asd",]
                if(is.call(x) && x[[1L]]==as.symbol(".")) x[[1L]] = quote(list) # decode `.()` to `list()`
                eval(x)
            }
            args = lapply(.dots, subarg)
            stopifnot(length(args) <= length(self$dims))
            names(args) = self$dims[seq_along(args)]
            completed = character()
            r = NULL
            # - [ ] check if all cols exists in dims, filters having NULL or vector are not checked as it refers to no filter and fact-filter
            cols_missing = sapply(names(args), function(arg) if(is.null(args[[arg]])) FALSE else if(!is.list(args[[arg]])) FALSE else !all(names(args[[arg]]) %in% names(self$db[[arg]])))
            if(any(cols_missing)) stop(sprintf("Field used in subset does not exists in dimensions %s.", paste(names(cols_missing)[cols_missing], collapse=", ")))
            # - [ ] categorize filters into fact-filters and dimension-filters
            fact_filter = sapply(args, function(x) !is.list(x) && length(x))
            dim_filter = sapply(args, function(x) (is.list(x) && length(x)) || is.null(x))
            # - [ ] check if binary search possible, only leading fact filters
            if(isTRUE(fact_filter[[1L]])){
                # data.table#1419 workaround
                nm = names(fact_filter)
                binsearch_cols = rleid(fact_filter)==1L
                names(binsearch_cols) = nm
                names(fact_filter) = nm
                # workaround end
                # - [ ] do binary search for key-leading cols for fact-filters
                r = self$db[[self$fact]][i = args[binsearch_cols], nomatch = 0L]
                completed = c(completed, names(binsearch_cols)[binsearch_cols])
            }
            # - [ ] fact-filter after the gap as vector scans
            if(any(!(ff <- names(fact_filter)[fact_filter]) %in% completed)){
                for(fff in ff[!ff %in% completed]){
                    qi = as.call(list(quote(`%in%`), as.name(self$keys[[fff]]), args[[fff]]))
                    r = if(is.null(r)) self$db[[self$fact]][eval(qi)] else r[eval(qi)]
                    completed = c(completed, fff)
                }
            }
            # - [ ] dimension-filters
            if(any(dim_filter)){
                # - [ ] for each dimension
                for(dim in names(dim_filter)[dim_filter]){
                    joincol = key(self$db[[dim]])
                    if(identical(args[[dim]],list(NULL))){ # lookup all columns from dim
                        qdim = quote(self$db[[dim]])
                        qfact = if(is.null(r)) quote(self$db[[self$fact]]) else quote(r)
                    } else {
                        dim_attrs = names(args[[dim]])
                        if(length(unique(dim_attrs))!=length(args[[dim]])) stop(sprintf("Dimensions hierarchy attributes has to be uniquely named `.(a1=..., a2=...)`, only lookup all dimension attributes does not require names and can be used as `.(NULL)`."))
                        dim_attrs_filter = sapply(dim_attrs, function(attr) !is.null(args[[dim]][[attr]]))
                        # - [ ] each filtered attribute in dimension
                        dim_attrs_filter_cols = names(dim_attrs_filter)[dim_attrs_filter]
                        dim_attrs_filter_calls = lapply(setNames(dim_attrs_filter_cols, dim_attrs_filter_cols), function(attr) as.call(list(quote(`%in%`), as.name(attr), args[[dim]][[attr]])))
                        qi = Reduce(function(a,b) bquote(.(a) & .(b)), dim_attrs_filter_calls)
                        # use data.table index on dimensions while filter - this has to be done on client side when creating cube
                        # prepare for join
                        qj = as.call(lapply(c("list", unique(c(joincol, dim_attrs))), as.symbol))
                        qdim = if(is.null(qi)) quote(self$db[[dim]][, eval(qj)]) else quote(self$db[[dim]][eval(qi), eval(qj)])
                        qfact = if(is.null(r)) quote(self$db[[self$fact]]) else quote(r)
                    }
                    r = eval(qfact)[eval(qdim), on = c(joincol), nomatch = 0L]
                    completed = c(completed, dim)
                }
            }
            return(r)
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
        }
    )
)

# as.cube -----------------------------------------------------------------

as.cube = function(x, ...){
    UseMethod("as.cube")
}

as.cube.default = function(x, ...){
    as.cube(as.array(x, ...))
}

as.cube.array = function(x, fact, dims, na.rm=TRUE, ...){
    dt = as.data.table(x, na.rm=na.rm)
    dim_cols = names(dt)[-length(dt)]
    dim_cols = if(missing(dims)){
        setNames(dim_cols, dim_cols)
    } else {
        stopifnot(is.character(dims), length(dims)==length(dim_cols))
        setNames(dim_cols, dims)
    }
    fact = if(missing(fact)) "fact" else stopifnot(is.character(fact), length(fact)==1L)
    cube$new(fact = setNames(list(dt), fact),
             dims = lapply(dim_cols, function(dim_col) setDT(setNames(list(unique(dt[[dim_col]])), dim_col), key = dim_col)))
}

as.cube.list = function(x, aggregate, ...){
    stopifnot(is.list(x), all(c("fact","dims") %in% names(x)))
    cube$new(fact = x$fact, dims = x$dims, aggregate = aggregate)
}

# capply ------------------------------------------------------------------

capply = aggregate.cube = function(x, MARGIN, FUN, ...){
    stopifnot(inherits(x, "cube"))
    x$apply(MARGIN, FUN, ...)
}

# `[.cube` ----------------------------------------------------------------

"[.cube" = function(x, ...){
    x$subset(.dots = match.call(expand.dots = FALSE)$`...`)
}
