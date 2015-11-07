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
each.i = function(int, i, keys){
    # preprocessing of `...` arg of `[.cube` and `i` arg of `[[.cube`
    stopifnot(is.integer(int), is.pairlist(i), is.character(keys))
    x = if(int > length(i)) list() else i[[int]] # fill missing args: x[,] to x[,,.()] in case of 3 dimensions
    if(missing(x)) x = list() # empty args: x[,,"asd"] to x[.(),.(),"asd"]
    else if(is.null(x)) x = setNames(list(NULL), keys[[int]]) # null args: x[NULL,NULL,"asd"] to x[.(NULL),.(NULL),"asd"]
    if(is.call(x) && x[[1L]]==as.symbol(".")) x[[1L]] = quote(list) # decode x[.(y)] to x[list(y)]
    x = eval.parent(x) # x[,,var] to x[,,.(keycol=c("val1","val2"))], x[,,c("asd","asd2")] to x[,,.(keycol=c("asd","asd2"))]
    if(is.atomic(x)) x = setNames(list(x), keys[[int]]) # x[,,"asd"] to x[,,.(keycol="asd")]
    stopifnot(is.list(x))
    if(length(x)) stopifnot(length(unique(names(x)))==length(names(x)))
    x
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
            stopifnot(is.list(x), as.logical(length(x)), all(c("fact","dims") %in% (names(x))))
            self$env = as.environment(x)
            invisible(self)
        },
        print = function(){
            prnt = character()
            prnt["head"] = "<cube>"
            fact.size = self$fapply(mb.size, simplify = TRUE)
            prnt["fact"] = sprintf("fact:\n  %s %s rows x %s cols (%.2f MB)", self$fact, self$fapply(nrow, simplify = TRUE), self$fapply(ncol, simplify = TRUE), fact.size)
            dims.size = self$dapply(mb.size, simplify = TRUE)
            if(length(self$dims)) prnt["dims"] = paste0("dims:\n", paste(sprintf("  %s %s rows x %s cols (%.2f MB)", self$dims, self$dapply(nrow, simplify = TRUE), self$dapply(ncol, simplify = TRUE), dims.size), collapse="\n"))
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
            lapply(setNames(seq_along(keys), names(keys)), each.i, i, keys)
        },
        subset = function(..., .dots){
            # - [x] catch dots, preprocess, evaluate
            if(missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            i = self$parse.i(.dots)
            return(i)
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
            null_subset = any(sapply(filter, identical, 0L))
            r = if(null_subset) self$db[[self$fact]][0L] else NULL
            if(!null_subset & isTRUE(fact_filter[[1L]])){
                # data.table#1419 workaround, fixed in 1.9.7
                nm = names(fact_filter)
                binarysearch_dims = rleid(fact_filter)==1L
                names(binarysearch_dims) = nm
                names(fact_filter) = nm
                # workaround end
                # - [x] do binary search for key-leading cols for fact-filters
                true_binarysearch_dims = names(binarysearch_dims)[binarysearch_dims]
                if(getOption("datacube.verbose", FALSE)) cat(sprintf("data.cube: Filtering fact using binary search on '%s'.\n", paste(true_binarysearch_dims, collapse=", ")))
                binsearch_vals = lapply(setNames(true_binarysearch_dims, self$keys[true_binarysearch_dims]), function(dim) dim.env[[dim]][[1L]])
                r = self$db[[self$fact]][i = do.call(CJ, binsearch_vals), nomatch = NA]
            } else binarysearch_dims = setNames(rep(FALSE, length(fact_filter)), names(fact_filter))
            # - [x] fact-filter after the gap as vector scans
            if(!null_subset & any(!(ff <- names(fact_filter)[fact_filter]) %in% binarysearch_dims[binarysearch_dims])){
                filter_after_gap = ff[!ff %in% binarysearch_dims[binarysearch_dims]]
                dim.id = lapply(setNames(filter_after_gap, self$keys[filter_after_gap]), function(dim) dim.env[[dim]][[1L]])
                qi = Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(dim.id), function(col) as.call(list(quote(`%in%`), as.name(col), dim.id[[col]]))))
                r = if(is.null(r)) self$db[[self$fact]][eval(qi)] else r[eval(qi)]
            }
            # - [x] return cube with all dimensions filtered
            as.cube(x = list(), fact = setNames(list(if(is.null(r)) copy(self$db[[self$fact]]) else r), self$fact), dims = as.list(dim.env)[self$dims])
        },
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
        drop = function() self
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
    #if(isTRUE(drop)) r$drop() else r
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
    x$dapply(nrow, simplify = TRUE)
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
