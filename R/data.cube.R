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
            stopifnot(is.fact(fact), all(sapply(dimensions, is.dimension)))
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
            #if(anyDuplicated(unlist(lkp_cols))) browser()#stop("Cannot lookup dimension attributes due to the column names duplicated between dimensions.")
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
                if(length(x)) stopifnot(!anyDuplicated(names(x))) # unique names
                x
            }
            keys = setNames(self$fact$id.vars, names(self$dimensions))
            i = lapply(setNames(seq_along(keys), names(keys)), parse.each.i, i, keys)
            # - [x] check if all cols exists in dims
            cols_missing = sapply(names(i), function(dim) !all(names(i[[dim]]) %in% self$dimensions[[dim]]$fields))
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
            dim.names = setNames(nm = names(self$dimensions))
            fact_filter = !sapply(dims.filter, is.null)
            if(!any(fact_filter)){
                return(self)
            }
            # returned object
            r = new.env()
            r$id.vars = self$fact$id.vars
            # - [x] filter dimensions and levels while quering them to new environment
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
                keys = self$dims.apply(function(x) x$data[[1L]], dims = names(fact_filter)[fact_filter])
                qi = Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(dims.keys), function(col) as.call(list(quote(`%in%`), as.name(col), dims.keys[[col]]))))
                # product = prod(sapply(keys, length))
                # if (product > 1e3L) {
                #     1
                # }
                r$fact = self$fact$subset()
                dims.keys = lapply(setNames(vectorscan_dims, keys[vectorscan_dims]), function(dim) r$dims[[dim]][[1L]])
                qi = Reduce(function(a, b) bquote(.(a) & .(b)), lapply(names(dims.keys), function(col) as.call(list(quote(`%in%`), as.name(col), dims.keys[[col]]))))
                r$fact[[self$fact]] = if(!length(r$fact[[self$fact]])) self$env$fact[[self$fact]][eval(qi)] else r$fact[[self$fact]][eval(qi)]
                # r$fact = self$fact$subset(..., drop = drop)
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
            as.data.cube(r)
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

#' @title Test if data.cube class
#' @param x object to tests.
is.data.cube = function(x) inherits(x, "data.cube")

#' @title Subset data.cube
#' @param x data.cube object
#' @param ... values to subset on corresponding dimensions, when wrapping in list it will refer to dimension hierarchies
#' @param drop logical, default TRUE, drop redundant dimensions, same as *drop* argument in \code{[.array}.
#' @return data.cube class object
"[.data.cube" = function(x, ..., drop = TRUE){
    if(!is.logical(drop)) stop("`drop` argument to data.cube subset must be logical. If argument name conflicts with your dimension name then provide it without name, elements in ... are matched by positions - as in array method - not names.")
    r = x$subset(.dots = match.call(expand.dots = FALSE)$`...`)
    if(isTRUE(drop)) r$drop() else r
    r
}

# @title Extract data.cube
# @param x data.cube object
# @param i list of values used to slice and dice on cube
# @param j expression to evaluate on fact
# @param by expression/character vector to aggregate measures accroding to *j* argument.
# @return data.cube?? class object
# "[[.data.cube" = function(x, i, j, by){
#     r = x$extract(by = by, .call = match.call())
#     r
# }

dimnames.data.cube = function(x){
    r = x$dims.apply(function(x) x$data[[1L]])
    if(!length(r)) return(NULL)
    r
}

str.data.cube = function(object, ...){
    print(object$schema())
    invisible()
}

# format.data.cube = function(x, measure.format = list(), dots.format = list(), dcast = FALSE, ...){
#     stopifnot(is.list(measure.format))
#     keys = x$dapply(key, simplify = TRUE)
#     measures = setdiff(names(x$env$fact[[x$fact]]), keys)
#     if(length(measure.format)) stopifnot(
#         sapply(measure.format, is.function),
#         length(names(measure.format))==length(measure.format), 
#         names(measure.format) %in% measures
#     )
#     if(length(keys)) r = setorderv(copy(x$env$fact[[x$fact]]), cols = keys, order=1L, na.last=TRUE) else {
#         stopifnot(nrow(x$env$fact[[x$fact]])==1L) # grant total
#         r = copy(x$env$fact[[x$fact]])
#     }
#     if(length(measure.format)){
#         for(mf in names(measure.format)){
#             FUN = measure.format[[mf]]
#             DOTS = dots.format[[mf]]
#             set(r, i = NULL, j = mf, value = FUN(r[[mf]], ... = DOTS))
#         }
#     }
#     if(isTRUE(dcast)) r = dcast.data.table(r, ...)
#     r[]
# }
