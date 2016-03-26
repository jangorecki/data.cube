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
        initialize = function(fact, dimensions, .env) {
            if (!missing(.env)){
                # skip heavy processing for env argument
                self$dimensions = .env$dimensions
                self$id.vars = .env$id.vars
                self$fact = .env$fact
                return(invisible(self))
            }
            stopifnot(is.fact(fact), all(sapply(dimensions, is.dimension)))
            self$dimensions = dimensions
            self$id.vars = unname(unlist(lapply(self$dimensions, `[[`, "id.vars"))) # first col must be primary key
            self$fact = fact
            invisible(self)
        },
        dim = function() {
            # fd = self$fact$dim() # heavy
            unname(sapply(self$dimensions, function(x) nrow(x$data)))
        },
        print = function() {
            dict = self$schema()
            prnt = character()
            prnt["header"] = "<data.cube>"
            #prnt["distributed"] = 
            n.measures = length(self$fact$measure.vars)
            prnt["fact"] = dict[type=="fact",
                                sprintf("fact%s:\n  %s rows x %s dimensions x %s measures (%.2f MB)", 
                                        if(!self$fact$local) sprintf(" (distributed on %s nodes)", length(attr(self$fact$data, "rscl"))) else "",
                                        nrow, ncol - n.measures, n.measures, mb)]
            if (length(self$dimensions)) {
                dt = dict[type=="dimension", .(nrow = nrow[is.na(entity)], ncol = ncol[is.na(entity)], mb = sum(mb, na.rm = TRUE)), .(name)]
                prnt["dims"] = paste0("dimensions:\n", paste(dt[, sprintf("  %s : %s entities x %s levels (%.2f MB)", name, nrow, ncol, mb)], collapse="\n"))
            }
            prnt["size"] = sprintf("total size: %.2f MB", dict[,sum(mb)])
            cat(prnt, sep = "\n")
            invisible(self)
        },
        # dims.apply
        dims.apply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE, dims = names(self$dimensions)) {
            FUN = match.fun(FUN)
            sapply(X = self$dimensions[dims],#lapply(setNames(nm = dims), function(dd) self$dimensions[[dd]]),
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        # fact.apply
        fact.apply = function(FUN, ..., simplify = FALSE, USE.NAMES = TRUE) {
            FUN = match.fun(FUN)
            sapply(X = list(self$fact),
                   FUN = FUN, ...,
                   simplify = simplify, USE.NAMES = USE.NAMES)
        },
        denormalize = function(dims = names(self$dimensions), na.fill = FALSE) {
            n.dims = length(dims)
            all_cols = lapply(self$dims.apply(function(x) lapply(x$levels, names.level), dims = dims), `[[`, 1L)
            key_cols = sapply(all_cols, `[[`, 1L)
            lkp_cols = lapply(all_cols, `[`, -1L)
            # that already solved later in `lookupv`
            #if(anyDuplicated(unlist(lkp_cols))) browser()#stop("Cannot lookup dimension attributes due to the column names duplicated between dimensions.")
            r = if (!na.fill | n.dims == 0L){
                copy(self$fact$data)
            } else {
                # `nomatch` to be extended after data.table#857 resolved
                self$fact$data[i = do.call(CJ, c(self$dims.apply(function(x) x$data[[1L]]), list(sorted = TRUE, unique = TRUE))), nomatch = NA, on = swap.on(key_cols)]
            }
            # lookup
            lookupv(dims = lapply(self$dimensions[dims], as.data.table.dimension), r)
            if (n.dims > 0L) setkeyv(r, unname(key_cols))[] else r[]
        },
        schema = function() {
            rbindlist(list(
                fact = rbindlist(list(fact = self$fact$schema()), idcol = "name"),
                dimension = rbindlist(lapply(self$dimensions, function(x) x$schema()), idcol = "name")
            ), idcol = "type")
        },
        head = function(n = 6L) {
            list(fact = self$fact$head(n = n), dimensions = lapply(self$dimensions, function(x) x$head(n = n)))
        },
        parse.i = function(i) {
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
        subset = function(..., .dots, drop = TRUE) {
            # - [x] catch dots, preprocess, evaluate
            if (missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            i.meta = self$parse.i(.dots)
            dims.filter = lapply(i.meta, build.each.i)
            # - [x]  no filters returns self
            dim.names = setNames(nm = names(self$dimensions))
            fact_filter = !sapply(dims.filter, is.null)
            if (!any(fact_filter)) {
                return(self)
            }
            # returned object
            r = new.env()
            r$id.vars = self$id.vars
            # - [x] filter dimensions and levels while quering them to new environment
            r$dimensions = lapply(dim.names, function(d) {
                if (d %in% names(fact_filter)[fact_filter]) self$dimensions[[d]]$subset(i.meta = i.meta[[d]], drop = drop) else self$dimensions[[d]]
            })
            # - [x] produce fact, handle NULL subset to returns empty fact
            if (any(sapply(dims.filter, identical, 0L))) {
                r$fact = as.fact(x = self$fact$data[0L], id.vars = self$fact$id.vars, measure.vars = self$fact$measure.vars, measures = self$fact$measures)
            } else {
                # - [x] get dimension base key values after dim filtering to subset fact table
                stopifnot(any(fact_filter)) # this should be true already
                fact_filter_cols = setNames(names(fact_filter)[fact_filter], nm = self$id.vars[fact_filter])
                keys = lapply(fact_filter_cols, function(d) r$dimensions[[d]]$data[[1L]])
                r$fact = self$fact$subset(keys, drop = drop)
            }
            # - [x] drop 1L element dimensions
            if (drop) {
                drop.dims = sapply(r$dimensions, function(d) dim(d)[1L]==1L) # take only PK of dimension
                r$dimensions[drop.dims] = NULL
                r$id.vars = r$id.vars[!drop.dims]
            }
            # - [x] return cube with all dimensions filtered and fact filtered
            as.data.cube(r)
        },
        setindex = function(drop = FALSE) {
            optional.logR = function(x, .log = getOption("datacube.log")) {
                if(isTRUE(.log)) eval.parent(substitute(logR(x), list(x = substitute(x)))) else x
            }
            r = list(
                fact = optional.logR(self$fact$setindex(drop=drop)),
                dimensions = lapply(self$dimensions, function(x) optional.logR(x$setindex(drop=drop)))
            ) # r - not used further but evaluated on lower classes
            invisible(self)
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
"[.data.cube" = function(x, ..., drop = TRUE) {
    if (!is.logical(drop)) stop("`drop` argument to data.cube subset must be logical. If argument name conflicts with your dimension name then provide it without name, elements in ... are matched by positions - as in array method - not names.")
    .dots = match.call(expand.dots = FALSE)$`...`
    r = x$subset(.dots = .dots, drop = drop)
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

dimnames.data.cube = function(x) {
    r = x$dims.apply(function(x) x$data[[1L]])
    if (!length(r)) return(NULL)
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
