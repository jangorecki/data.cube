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
            self$env$fact[[self$fact]]
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
        dapply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE, dims = self$dims){
            FUN = match.fun(FUN)
            sapply(X = lapply(selfNames(dims), function(x) self$env$dims[[x]]),
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        fapply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE){
            FUN = match.fun(FUN)
            sapply(X = self$env$fact,
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
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
    r = x$subset(.dots = match.call(expand.dots = FALSE)$`...`)
    browser()
    if(!is.logical(drop)) stop("`drop` argument to cube subset must be logical. If argument name conflicts with your dimension name then provide it without name, elements in ... are matched by positions - as in array method - not names.")
    r = x$subset(.dots = match.call(expand.dots = FALSE)$`...`)
    if(isTRUE(drop)) r$drop() else r
}

#' @title Extract cube
#' @param x cube object
#' @param i list of values used to slice and dice on cube
#' @param j expression to evaluate on fact
#' @param by expression/character vector to aggregate measures accroding to *j* argument.
#' @return Cube class object
"[[.cube" = function(x, i, j, by){
    cube.call = match.call()
    cube.call
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
    x[[j = lapply(.SD, FUN, ...), by = MARGIN]]
}
