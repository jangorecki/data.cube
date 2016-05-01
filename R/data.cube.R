#' @title OLAP cube data type
#' @description Extends array for OLAP operations on multidimensional hierarchical data powered by data.table.
#' @details
#' Implements OLAP cubes as data.cube object, set of data.tables objects. Aims for scalable real-time performance.
#' @seealso \code{\link{as.data.cube}}, \code{\link{data.cube}}, \code{\link{[.data.cube}}, \code{\link{fact}}, \code{\link{dimension}}
#' @docType package
#' @author Jan Gorecki
#' @references \href{https://stackoverflow.com/questions/35472639/star-schema-normalized-dimensions-denormalized-hierarchy-level-keys}{data model inside data.cube}
#' @references \href{https://github.com/Rdatatable/data.table/wiki}{data.table}
#' @name data.cube-package
NULL

#' @title data.cube class
#' @docType class
#' @format An R6 class object.
#' @details Class stores fact class and dimension classes. R6 class preview with \code{ls.str()}. For all tables in the model of cube use \code{str()}.  
#' @seealso \code{\link{as.data.cube}}, \code{\link{[.data.cube}}, \code{\link{fact}}, \code{\link{dimension}}
data.cube = R6Class(
    classname = "data.cube",
    public = list(
        fact = NULL,
        id.vars = character(),
        dimensions = list(),
        initialize = function(fact, dimensions, .env) {
            if (!missing(.env)) {
                # skip heavy processing for env argument
                self$dimensions = .env$dimensions
                self$id.vars = .env$id.vars
                self$fact = .env$fact
                return(invisible(self))
            }
            stopifnot(is.fact(fact), all(sapply(dimensions, is.dimension)))
            # - [ ] dimless input: only grand total value would be valid
            if (!length(fact$id.vars)) {
                if (length(dimensions)) warning("Provided fact has no key columns, all dimensions are being dropped.", call.=FALSE)
                self$dimensions = structure(list(), .Names = character(0))
                self$id.vars = fact$id.vars
                self$fact = fact
                return(invisible(self))
            }
            # - [ ] dimensions metadata validation
            uniquely.named = function(x) !is.null(names(x)) && !anyDuplicated(names(x)) && !any(names(x)=="")
            if (!uniquely.named(dimensions)) stop("Dimensions must be uniquely named.")
            if ("grouping" %chin% names(dimensions)) stop("Dimension should not be named 'grouping' as this name is reserved for dimension binded during `rollup` processing.")
            dims.id.vars = sapply(dimensions, `[[`, "id.vars", simplify=FALSE)
            if (any((dims.id.vars.len <- sapply(dims.id.vars, length)) > 1L)) stop(sprintf("Dimensions must have non-composite keys defined `length(d$id.vars)==1L`: %s. That may change after 'add time variant support ' data.cube#11 solved.", paste(names(dims.id.vars.len)[dims.id.vars.len > 1L], collapse=", ")))
            dims.id.vars = unlist(dims.id.vars)
            extra.dims = setdiff(dims.id.vars, fact$id.vars)
            missing.dims = setdiff(fact$id.vars, dims.id.vars)
            # - [ ] drop extra dimensions vs fact
            if (length(missing.dims)) {
                message(sprintf(
                    "%s/%s of provided dimensions does not have corresponding key in provided fact, they will be dropped: %s",
                    length(missing.dims), length(dims.id.vars),
                    paste(missing.dims, collapse = ", ")
                ))
                dimensions[missing.dims] = NULL
                dims.id.vars = setdiff(dims.id.vars, missing.dims)
            }
            # - [ ] drop extra fact keys vs dimensions
            if (length(extra.dims)) {
                message(sprintf(
                    "%s/%s of the keys in provided fact does not have corresponding dimension(s) provided, they will be dropped (%s) and fact will get aggregated to remaining dimenions.",
                    length(extra.dims), length(fact$id.vars),
                    paste(extra.dims, collapse = ", ")
                ))
                fact = as.fact(x=fact$data, id.vars=setdiff(fact$id.vars, extra.dims), measure.vars=fact$measure.vars, measures=fact$measures)
            }
            # - [ ] reorder dimensions to match id.vars order in fact
            dims.order = chmatch(dims.id.vars, fact$id.vars)
            stopifnot(identical(fact$id.vars, unname(dims.id.vars)[dims.order]))
            self$dimensions = dimensions[dims.order]
            self$id.vars = unname(dims.id.vars)[dims.order]
            self$fact = fact
            invisible(self)
        },
        dim = function() {
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
        denormalize = function(na.fill = FALSE, dims = names(self$dimensions)) {
            r = as.data.table(self$fact)
            if (isTRUE(na.fill)) {
                # `nomatch` to be extended to raise error if fact has values not in dims, after data.table#857 resolved
                dn = dimnames(self)
                r = r[i=do.call(CJ, c(dn, list(sorted=TRUE, unique=TRUE))),
                      nomatch=NA,
                      on=setNames(names(dn), nm=self$fact$id.vars)]
            }
            # lookup
            lookupv(dims = lapply(self$dimensions[dims], as.data.table.dimension), r)
            if (length(self$fact$id.vars)) setkeyv(r, self$fact$id.vars)[] else r[]
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
        # `[.data.cube`
        parse.dots = function(dots) {
            dims = names(self$dimensions)
            # - [x] handling empty input `dc[]` to `dc[dima=list(), dimb=list(), dimc=list()]` - this is already handled in "[.data.cube"
            if (is.null(dots)) return(sapply(dims, function(dim) list(), simplify=FALSE))
            # - [x] there must be no more slices than dimensions in data.cube
            if (length(dots) > length(self$dimensions)) stop(sprintf("You provided to many dimensions to data.cube subset. Number of dimensions in data.cube is %s while there were %s dimensions on input.", length(self$dimensions), length(dots)))
            # - [x] fill empty args `dc[,,"c"]` to `dc[list(), list(), "c"]`
            empty.args = which(sapply(dots, identical, substitute()))
            if (length(empty.args)) dots[empty.args] = lapply(empty.args, function(i) list())
            # - [x] fill in missing trailing dimensions `dc["a"]` to `dc["a", list(), list()]`
            if (length(dots) < length(self$dimensions)) {
                ii = (length(dots)+1L):length(self$dimensions)
                dots[ii] = lapply(ii, function(i) list())
            }
            # - [x] slices can be reorderd `dc[dimb = "b", list(), dima = "a"]` to `dc[dima = "a", dimb = "b", dimc=list()]`
            dots.dims = names(dots)
            if (is.null(dots.dims)) {
                # - [x] fill dim names when all dims are unnamed, matching by position
                dots.dims = dims[seq_along(dots)]
                setattr(dots, "names", dots.dims)
            } else {
                # - [x] fill dim names for partially unnamed dimensions, matching by position, validate non existing dims and unique dim names
                named.dots.dims = dots.dims!=""
                if (length(dots.dims[named.dots.dims]) != uniqueN(dots.dims[named.dots.dims])) stop("Dimension names of arguments provided to data.cube subset are not unique")
                if (length(bad.dims <- setdiff(dots.dims[named.dots.dims], dims))) stop(sprintf("Dimension names of argument provided to data.cube subset does not exists for that data.cube: %s.", paste(bad.dims, collapse=", ")))
                # skip if all dots.dims names provided
                if (!all(named.dots.dims)) {
                    remaining.dims = setdiff(dims, dots.dims[named.dots.dims])
                    stopifnot(length(remaining.dims) == sum(!named.dots.dims)) # length of dots should already be expanded for empty `list()` for all dimensions
                    dots.dims[!named.dots.dims] = remaining.dims
                    setattr(dots, "names", dots.dims)
                }
            }
            stopifnot(identical(sort(dims), sort(dots.dims)))
            if (!identical(dims, dots.dims)) {
                # reorder
                dots = dots[dims]
                dots.dims = dims
            }
            stopifnot(identical(names(dots), names(self$dimensions)))
            # - [x] decode `.`/`+` to `list`
            dot.to.list = sapply(dots, function(x) is.call(x) && as.character(x[[1L]]) %chin% ".")
            plus.to.list = sapply(dots, function(x) is.call(x) && as.character(x[[1L]]) %chin% "+")
            for (i in which(dot.to.list)) {
                if (length(dots[[i]])==2L && identical(dots[[i]][[2L]], quote(`(`(`.`)))) {
                    # - [x] support `dc[.(.)]` for consistency with `+(.)`
                    dots[[i]] = quote(list())
                } else {
                    dots[[i]][[1L]] = quote(list)
                }
            }
            for (i in which(plus.to.list)) {
                if (length(dots[[i]])==2L && identical(dots[[i]][[2L]], quote(`(`(`.`)))) {
                    # - [ ] allows to use `dc[+(.)]` - this is really `+`(`(`(`.`))
                    dots[[i]] = quote(list())
                } else {
                    dots[[i]][[1L]] = quote(list)
                }
            }
            # - [x] array like slices: `dc["a", list(), "c"]` to `dc[list(dimakey = "a"), list(), list(dimckey = "c")]`
            # - [x] `dc[NULL, list(), list()]` to `dc[list(dimakey = NULL), list(), list()]`
            # - [x] `dc[var.a, list(), var.c]` to `dc[list(dimakey = "a"), list(), list(dimckey = "c")]`
            dots = sapply(dots.dims, function(dim) {
                r = eval(dots[[dim]], parent.frame(), parent.frame())
                if (is.language(r) || is.function(r)) {
                    r = deparse(r, width.cutoff=500L)
                    stop(sprintf("Invalid input provided for '%s' dimension: %s", dim, toString(r)), call.=FALSE)
                }
                if (!is.atomic(r) && !length(r)) return(r) # list()
                dimkey = self$dimensions[[dim]]$id.vars
                dimfields = self$dimensions[[dim]]$fields
                if (is.atomic(r)) r = setNames(list(r), dimkey)
                if (!is.list(r) || !identical(class(r), "list")) stop(sprintf("Invalid input provided for '%s' dimension: %s", dim, toString(r)), call.=FALSE)
                if (is.null(names(r))) {
                    if (length(r) > 1L) stop(sprintf("When subset data.cube by '%s' dimension you can use only single unnamed field, it will map to dimension key.", dim), call.=FALSE)
                    names(r) = ""
                }
                empty.field.names = names(r)==""
                # - [ ] TODO change to `anyDuplicated`
                if (length(field.names <- names(r)[!empty.field.names]) != uniqueN(field.names)) stop(sprintf("Fields to subset in '%s' dimension are not uniquely named.", dim), call.=FALSE)
                if (sum(empty.field.names) > 1L) stop(sprintf("There are multiple unnamed fields provided to '%s' dimension on data.cube subset. Only one unnamed field is allowed which maps to dimension key.", dim))
                if (sum(empty.field.names) == 1L) {
                    if (dimkey %chin% names(r)) stop(sprintf("When subset data.cube by '%s' dimension there can be only one unnamed field provided that maps to dimension key, so dimension key field must not be provided at the same time.", dim), call.=FALSE)
                    names(r)[which(empty.field.names)] = dimkey
                }
                if (length(fields.not.exists <- setdiff(names(r), dimfields))) stop(sprintf("Field name provided to '%s' dimension does not exists in that dimension: %s.", dim, paste(fields.not.exists, collapse=", ")), call.=FALSE)
                r
            }, simplify=FALSE)
            
            # - [x] return operation (filter or dim collapse, potentially rollup?) and values
            ops = c(".", "+")[plus.to.list + 1L]
            list(ops = setNames(ops, dots.dims), sub = dots)
        },
        subset = function(..., .dots, drop = TRUE) {
            # - [x] catch dots, preprocess, evaluate
            if (missing(.dots)) .dots = match.call(expand.dots = FALSE)$`...`
            i.meta = self$parse.dots(.dots)
            i.ops = i.meta$ops
            i.sub = i.meta$sub
            # exit on `dc[.(),.(),.()]` considering drop, exit from `dc[]` wont use drop and is handled in "[.data.cube" function
            if (all(sapply(i.sub, identical, list())) && all(i.ops==".")) return(
                if (drop) {
                    drop.dims = sapply(self$dimensions, nrow)==1L
                    if (sum(drop.dims)) {
                        local.dc = self$clone(deep = TRUE)
                        local.dc$dimensions[drop.dims] = NULL
                        local.dc$id.vars = self$id.vars[!drop.dims]
                        local.dc
                    } else self
                } else self
            )
            # returned object
            r = new.env()
            # - [x] filter dimensions and levels while quering them to new environment
            r$dimensions = sapply(names(i.sub), function(dim) {
                self$dimensions[[dim]]$subset(i.sub = i.sub[[dim]])
            }, simplify=FALSE)
            r$id.vars = self$id.vars
            # - [x] filter fact - prepare index for subset fact
            filter.dims = sapply(i.sub, function(x) length(x) || is.null(x))
            filter.dims = names(filter.dims)[as.logical(filter.dims)]
            # primary keys of dimensions after filtering
            dimkeys = sapply(names(r$dimensions)[names(r$dimensions) %chin% filter.dims], function(dim) {
                r$dimensions[[dim]]$data[[r$dimensions[[dim]]$id.vars]]
            }, simplify=FALSE)
            stopifnot(names(dimkeys) %chin% names(r$dimensions)) # all names must match, before drop dims
            # - [x] drop sliced dimensions
            if (drop) {
                len1.dims = names(dimkeys)[sapply(dimkeys, length)==1L]
                # if user provides multiple values to dimension filter key, it should not drop that dim even when only 1L was matched, base::array raises error on nomatch
                filter.multkey = len1.dims[sapply(len1.dims, function(dim) length(i.sub[[dim]][[r$dimensions[[dim]]$id.vars]])) > 1L]
                if.drop = names(r$dimensions) %chin% setdiff(len1.dims, filter.multkey)
                r$dimensions[if.drop] = NULL
                r$id.vars = r$id.vars[!if.drop]
            }
            # - [x] subset fact
            dimcols = self$id.vars[names(self$dimensions) %chin% names(dimkeys)]
            stopifnot(length(dimcols) == length(dimkeys))
            setattr(dimkeys, "names", dimcols)
            collapse.dims = names(i.ops)[i.ops=="+"]
            collapse.cols = self$id.vars[names(self$dimensions) %chin% collapse.dims]
            r$fact = self$fact$subset(dimkeys, collapse=collapse.cols, drop=drop)
            stopifnot(ncol(r$fact$data) > 0L, length(collapse.cols)==length(collapse.dims))
            if (length(collapse.dims)) {
                r$dimensions[collapse.dims] = NULL
                r$id.vars = setdiff(r$id.vars, collapse.cols)
            }
            # - [x] return cube with all dimensions filtered and fact filtered
            as.data.cube.environment(r)
        },
        # setindex
        setindex = function(drop = FALSE) {
            optional.logR = function(x, .log = getOption("datacube.log")) {
                if(isTRUE(.log)) eval.parent(substitute(logR(x), list(x = substitute(x)))) else x
            }
            r = list(
                fact = optional.logR(self$fact$setindex(drop=drop)),
                dimensions = lapply(self$dimensions, function(x) optional.logR(x$setindex(drop=drop)))
            ) # r - not used further but evaluated on lower classes
            invisible(self)
        },
        rollup = function(by) {
            # get relevant dims
            guess.dim = function(x) {
                stopifnot(is.character(x))
                if (!length(x)) return(character(0))
                self$dime
            }
            # for each dimension in 'by'
            ### outsource to dimension$rollup?
            ## get relevant levels
            guess.level = function(x) {
                stopifnot(is.character(x))
                if (!length(x)) return(character(0))
            }
            ## get relevant hierarchies
            guess.hierarchies = function(x) {
                stopifnot(is.character(x))
                if (!length(x)) return(character(0))
            }
            ## define level for new dimension base OR create new, if rollup on 2+ hierarchies
            ## update hierarchies, drop redundant levels
            
            ### outsource fact$rollup ?
            
            # lookup fact for new dimensions keys
            # rollup.data.table over dims keys
            # get `grouping` field in rollup results to create grouping dimension
            # bind grouping dimension
        }
    )
)

#' @title Test if data.cube class
#' @param x object to tests.
is.data.cube = function(x) inherits(x, "data.cube")

#' @title Subset data.cube
#' @param x data.cube object
#' @param ... values to subset on corresponding dimensions, when wrapping in list it will refer to dimension hierarchies
#' @param drop logical, default TRUE, drop redundant dimensions, same as \emph{drop} argument in \code{[.array}.
#' @return data.cube class object
"[.data.cube" = function(x, ..., drop = TRUE) {
    if (!is.logical(drop)) stop("`drop` argument to data.cube subset must be logical. If argument name conflicts with your dimension name then provide it without name, elements in ... are matched by positions - as in array method - not names.")
    # missingness of `drop` in subset call affects `...` catched by match.call, so extra checks below
    sub.call = match.call(expand.dots = FALSE)
    .dots = sub.call$`...`
    # exit when `dc[drop=.]` without handling `drop` arg, as ignored in base array, but here raise warning saying the drop argument was ignored
    drop.no.dots = "drop" %chin% names(sub.call) && is.null(.dots)
    if (drop.no.dots) {
        warning("drop argument is ignored for calls `dc[drop=.]` for consistency to base::array, for `drop` and empty slices use `dc[, drop=.]`.")
        return(x)
    }
    # exit when `dc[]` without handling `drop` arg, as ignored in base array
    no.drop.no.dots = !"drop" %chin% names(sub.call) && is.pairlist(.dots) && length(.dots)==1L && identical(.dots[[1L]], substitute())
    if (no.drop.no.dots) {
        return(x)
    }
    # proceed subset, also proceed empty subset `dc[,]` or `dc[, drop=.]`
    r = x$subset(.dots = .dots, drop = drop)
    r
}

# @title Extract data.cube
# @param x data.cube object
# @param i list of values used to slice and dice on cube
# @param j expression to evaluate on fact
# @param by expression/character vector to aggregate measures accroding to \emph{j} argument.
# @return data.cube?? class object
# "[[.data.cube" = function(x, i, j, by) {
#     r = x$extract(by = by, .call = match.call())
#     r
# }

dimnames.data.cube = function(x) {
    r = sapply(x$dimensions, dimnames, simplify=FALSE)
    if (!length(r)) return(NULL)
    r
}

str.data.cube = function(object, ...) {
    print(object$schema())
    invisible()
}

format.data.cube = function(x, na.fill = FALSE, measure.format = list(), dots.format = list(), dcast = FALSE, ...) {
    stopifnot(is.data.cube(x), is.list(measure.format), is.logical(dcast))
    id.vars = x$id.vars
    measure.vars = x$fact$measure.vars
    if (length(measure.format)) stopifnot(
        sapply(measure.format, is.function),
        length(names(measure.format))==length(measure.format),
        names(measure.format) %in% measure.vars
    )
    r = x$denormalize(dims = character(0), na.fill = na.fill)
    if (length(id.vars)) r = setorderv(r, cols = id.vars, order=1L, na.last=TRUE) 
    if (!is.null(measure.format)) { # measure.format=NULL will stop any formatting
        for (mv in measure.vars) {
            if (mv %chin% names(measure.format)) {
                FUN = measure.format[[mv]]
                set(r, i = NULL, j = mv, value = FUN(r[[mv]], ... = dots.format[[mv]]))
            } else {
                if (!is.null(FUN <- x$fact$measures[[mv]]$fun.format)) {
                    set(r, i = NULL, j = mv, value = FUN(r[[mv]], ... = dots.format[[mv]]))
                }
            }
        }
    }
    if (isTRUE(dcast)) r = dcast.data.table(r, ...)
    r[]
}

head.data.cube = function(x, n = 6L, ...) x$head(n)

length.data.cube = function(x) as.integer(nrow(x$fact))
names.data.cube = function(x) as.character(names(x$fact))
dim.data.cube = function(x) as.integer(x$dim())

#' @title Apply function over data.cube 
#' @param X data.cube object
#' @param MARGIN character or integer, dimensions by which aggregate is made
#' @param FUN function, by default it will apply \code{fun.aggregate} defined for each measure
#' @param ... arguments passed to \emph{FUN}
#' @description Wraps to \code{[.data.cube}.
#' @note When \code{FUN} argument was used, new data.cube is created with new measures.
apply.data.cube = function(X, MARGIN, FUN, ...) {
    if (!is.integer(MARGIN) && is.numeric(MARGIN)) MARGIN = as.integer(MARGIN) # 1 -> 1L
    if (is.integer(MARGIN)) MARGIN = X$id.vars[MARGIN] # 1L -> colnames[1L]
    stopifnot(is.data.cube(X), is.character(MARGIN), MARGIN %chin% X$id.vars)
    sub.fun = substitute(FUN)
    if (!missing(FUN)) {
        X = X$clone()
        .dots = match.call(expand.dots = FALSE)$`...`
        X$fact$measures = lapply(setNames(nm = X$fact$measure.vars), function(.var) {
            eval(as.call(c(
                list(
                    as.name("as.measure"),
                    x = .var,
                    label = character(0),
                    fun.aggregate = sub.fun
                ),
                .dots
            ))) # new measures with new fun.aggregate
        })
    }
    eval(as.call(c(
        list(
            as.name("["),
            as.name("X")
        ),
        lapply(unname(X$id.vars), function(x) {
            if (x %chin% MARGIN) substitute() else call("+")
        }),
        list( # required for consistency of <= 1 element dims
            drop = FALSE
        )
    )))
}
