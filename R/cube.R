is.unique = function(x) length(x)==length(unique(x))
selfNames = function(x) setNames(x, x)
mb.size = function(x) as.numeric(object.size(x))/(1024^2)
lookup = function(fact, dim, cols){
    if(any(cols %in% names(fact))) stop(sprintf("Column name collision on lookup for '%s' columns.", paste(cols[cols %in% names(fact)], collapse=", ")))
    fact[dim, (cols) := mget(paste0("i.", cols)), on = c(key(dim))]
    # workaround for data.table#1166 - lookup NAs manually
    if(all(!cols %in% names(fact))) fact[, (cols) := as.list(dim[0L, cols, with=FALSE][1L])]
    TRUE
}
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
build.each.i = function(dim.i){
    build.each.i.attr = function(attr) if(is.null(dim.i[[attr]])) 0L else as.call(list(quote(`%in%`), as.name(attr), dim.i[[attr]]))
    Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(dim.i), build.each.i.attr))
}

# cube --------------------------------------------------------------------

#' @title OLAP cube class
#' @docType class
#' @format An R6 class object.
#' @name cube
#' @details Generates cube class objects.
#' @aliases data.cube
cube = R6Class(
    classname = "cube",
    public = list(
        env = NULL,
        initialize = function(x){
            self$env = as.environment(x)
            invisible(self)
        },
        print = function(){
            prnt = character()
            prnt["head"] = "<cube>"
            fact.size = self$fapply(mb.size, simplify = TRUE)
            prnt["fact"] = sprintf("fact:\n  %s %s rows x %s cols (%.2f MB)", self$fact, self$fapply(nrow, simplify = TRUE), self$fapply(ncol, simplify = TRUE), fact.size)
            if(length(self$dims)){
                dims.size = self$dapply(mb.size, simplify = TRUE)
                prnt["dims"] = paste0("dims:\n", paste(sprintf("  %s %s rows x %s cols (%.2f MB)", self$dims, self$dapply(nrow, simplify = TRUE), self$dapply(ncol, simplify = TRUE), dims.size), collapse="\n"))
            } else dims.size = 0
            prnt["size"] = sprintf("total size: %.2f MB", sum(c(fact.size, dims.size)))
            cat(prnt, sep="\n")
        },
        # dim apply
        dapply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE, dims = self$dims){
            FUN = match.fun(FUN)
            sapply(X = lapply(selfNames(dims), function(x) self$env$dims[[x]]),
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        # fact apply
        fapply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE){
            FUN = match.fun(FUN)
            sapply(X = self$env$fact,
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        # methods
        denormalize = function(dims = self$dims, na.fill = FALSE){
            all_cols = self$dapply(names, dims = dims)
            key_cols = sapply(all_cols, `[`, 1L)
            lkp_cols = lapply(all_cols, `[`, -1L)
            if(!is.unique(unlist(lkp_cols))) stop("Cannot lookup dimension attributes due to the column names duplicated between dimensions.")
            r = if(!na.fill | length(dims)==0L){
                copy(self$env$fact[[self$fact]])
            } else {
                self$env$fact[[self$fact]][i = do.call(CJ, self$dapply(`[[`,1L, dims=dims)), nomatch=NA, on = c(key_cols[dims])]
            }
            sapply(dims[as.logical(sapply(lkp_cols, length))], function(dim) lookup(r, self$env$dims[[dim]], lkp_cols[[dim]]))
            if(length(dims)) setkeyv(r, unname(key_cols[dims]))[] else r[]
        },
        parse.i = function(i){
            keys = self$dapply(key, simplify = TRUE)
            i = lapply(setNames(seq_along(keys), names(keys)), parse.each.i, i, keys)
            # - [x] check if all cols exists in dims
            cols_missing = sapply(names(i), function(dim) !all(names(i[[dim]]) %in% names(self$env$dims[[dim]])))
            if(any(cols_missing)) stop(sprintf("Field used in query does not exists in dimensions '%s'.", paste(names(cols_missing)[cols_missing], collapse=", ")))
            i
        },
        # [.cube
        subset = function(..., .dots){
            # - [x] catch dots, preprocess, evaluate
            if(missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            i = self$parse.i(.dots)
            dims.filter = lapply(i, build.each.i)
            # - [x] iterate over dimensions - cleaner than mapply
            r = new.env()
            r$fact = list()
            r$dims = list()
            keys = self$dapply(key, simplify = TRUE)
            for(dim in self$dims){
                if(getOption("datacube.verbose", FALSE)) cat(sprintf("data.cube: processing dimension '%s'.\n", dim))
                r$dims[[dim]] = if(is.null(dims.filter[[dim]])) copy(self$env$dims[[dim]]) else self$env$dims[[dim]][eval(dims.filter[[dim]])]
                setkeyv(r$dims[[dim]], keys[[dim]])
            }
            # - [x] NULL subset returns empty fact
            if(any(sapply(dims.filter, identical, 0L))){
                r$fact[[self$fact]] = self$env$fact[[self$fact]][0L]
                return(as.cube(r))
            }
            fact_filter = !sapply(dims.filter, is.null)
            # - [x]  no filters returns copy of fact
            if(all(!fact_filter)){
                r$fact[[self$fact]] = copy(self$env$fact[[self$fact]])
                return(as.cube(r))
            }
            # - [x] check if binary search possible, only leading fact filters
            if(isTRUE(fact_filter[[1L]])){
                fact_filter2 = copy(fact_filter) # data.table#1419 rleid workaround as `copy()`, fixed in 1.9.7
                binarysearch_dims = self$dims[rleid(fact_filter2)==1L]
                if(getOption("datacube.verbose", FALSE)) cat(sprintf("data.cube: filter facts using binary search on '%s'.\n", paste(binarysearch_dims, collapse=", ")))
                r$fact[[self$fact]] = self$env$fact[[self$fact]][i = do.call(CJ, lapply(selfNames(binarysearch_dims), function(dim) r$dims[[dim]][[1L]])), nomatch = NA]
            } else binarysearch_dims = character()
            # - [x] fact-filter after the gap as vector scans
            vectorscan_dims = setdiff(self$dims[fact_filter], binarysearch_dims)
            if(length(vectorscan_dims)){
                if(getOption("datacube.verbose", FALSE)) cat(sprintf("data.cube: filter facts using vector scan on '%s'.\n", paste(vectorscan_dims, collapse=", ")))
                dims.keys = lapply(setNames(vectorscan_dims, keys[vectorscan_dims]), function(dim) r$dims[[dim]][[1L]])
                qi = Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(dims.keys), function(col) as.call(list(quote(`%in%`), as.name(col), dims.keys[[col]]))))
                r$fact[[self$fact]] = if(!length(r$fact[[self$fact]])) self$env$fact[[self$fact]][eval(qi)] else r$fact[[self$fact]][eval(qi)]
            }
            # - [x] return cube with all dimensions filtered and fact filtered
            return(as.cube(r))
        },
        # [[.cube
        extract = function(i, j, by, .call){
            if(!missing(.call)){
                i = .call[["i"]]
                j = .call[["j"]]
                by = .call[["by"]]
            } else stop("direct access to 'extract' method not yet supported")
            if(!is.null(i)){
                if(!(i[[1L]]==as.symbol(".") | i[[1L]]==quote(list))) stop("Argument `i` to `[[.cube` must be a call `list()` or `.()`.")
                i = as.list(i)[-1L]
                keep_dims = names(i)
                stopifnot(keep_dims %in% self$dims)
                all.i = sapply(self$dims, function(x) call("list"), simplify = FALSE)
                all.i[keep_dims] = i
                i = self$parse.i(as.pairlist(all.i))[keep_dims]
            }
            i
        },
        # drop used in [.cube
        drop = function(drop=1L){
            # Direct access to cube object method by `$drop()` should not be used on cubes that shares dimensions, you can use drop arg in `[.cube` safely
            # - [x] drop dimensions where cardinality = 1
            if(nrow(self$env$fact[[self$fact]])){
                cardinality = self$env$fact[[self$fact]][, lapply(.SD, uniqueN), .SDcols = c(unname(self$dapply(key, simplify = TRUE)))]
                dims_to_drop = sapply(setNames(cardinality, self$dims), `==`, 1L)
            } else {
                dims_to_drop = self$dapply(function(dim) nrow(dim)==1L, simplify = TRUE)
            }
            if(any(dims_to_drop)){
                dims_to_drop = names(dims_to_drop)[dims_to_drop]
                keys_to_drop = unname(self$dapply(key, dims = dims_to_drop, simplify = TRUE))
                self$env$dims[dims_to_drop] = NULL
                self$env$fact[[self$fact]][, c(keys_to_drop) := NULL]
            }
            self
        }
    ),
    active = list(
        fact = function() names(self$env$fact),
        dims = function() names(self$env$dims)
    )
)

# *.cube ----------------------------------------------------------------

#' @title Subset cube
#' @param x cube object
#' @param ... values to subset on corresponding dimensions, when wrapping in list it will refer to dimension hierarchy
#' @param drop logical, default TRUE, drop dimensions same as *drop* argument in `[.array`.
#' @return Cube class object
"[.cube" = function(x, ..., drop = TRUE){
    if(!is.logical(drop)) stop("`drop` argument to cube subset must be logical. If argument name conflicts with your dimension name then provide it without name, elements in ... are matched by positions - as in array method - not names.")
    r = x$subset(.dots = match.call(expand.dots = FALSE)$`...`)
    if(isTRUE(drop)) r$drop() else r
    r
}

#' @title Extract cube
#' @param x cube object
#' @param i list of values used to slice and dice on cube
#' @param j expression to evaluate on fact
#' @param by expression/character vector to aggregate measures accroding to *j* argument.
#' @return Cube class object
"[[.cube" = function(x, i, j, by){
    r = x$extract(.call = match.call())
    r
}

is.cube = function(x) inherits(x, "cube")

dim.cube = function(x){
    unname(x$dapply(nrow, simplify = TRUE))
}

dimnames.cube = function(x){
    x$dapply(`[[`,1L)
}

str.cube = function(object, ...){
    NextMethod()
    cat("cube$env$fact: ")
    str(object$env$fact, max.level = 1L, give.attr = FALSE)
    cat("cube$env$dims: ")
    str(object$env$dims, max.level = 1L, give.attr = FALSE)
    invisible()
}

# capply ------------------------------------------------------------------

#' @title Apply function on measures while aggregate on cube dimensions
#' @param x cube object
#' @param MARGIN character or list
#' @param FUN function
#' @param ... arguments passed to *FUN*
#' @description Wrapper around `[[.cube` and `j`, `by` arg.
capply = aggregate.cube = function(x, MARGIN, FUN, ...){
    stopifnot(inherits(x, "cube"), !missing(MARGIN), !missing(FUN))
    FUN = match.fun(FUN)
    x[[i = .(), j = lapply(.SD, FUN, ...), by = MARGIN]]
}
