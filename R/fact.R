#' @title Fact class
#' @docType class
#' @format An R6 class object.
#' @details Class stores fact table as local \code{data.table} or remote \code{big.data.table}. Measures can be provided manually to \code{measures} argument, useful for custom aggregate function per measure.
#' @seealso \code{\link{as.fact}}, \code{\link{measure}}, \code{\link{dimension}}, \code{\link{data.cube}}
fact = R6Class(
    classname = "fact",
    public = list(
        local = logical(),
        id.vars = character(), # foreign keys
        measure.vars = character(),
        measures = list(),
        data = NULL,
        initialize = function(x, id.vars = character(), measure.vars = character(), fun.aggregate = sum, ..., measures = NULL, .env) {
            if (!missing(.env)) {
                # skip heavy processing for env argument
                self$local = .env$local
                self$id.vars = .env$id.vars
                self$measure.vars = .env$measure.vars
                self$measures = .env$measures
                self$data = .env$data
                return(invisible(self))
            }
            sub.fun = substitute(fun.aggregate)
            stopifnot(is.character(id.vars), is.character(measure.vars))
            self$id.vars = unname(id.vars)
            # - [x] `fact$new` creates measures, or use provided in `measures` argument
            if (!is.null(measures)) {
                self$measures = measures
                self$measure.vars = names(self$measures)
            } else {
                if(!length(measure.vars)) stop("You need to provide at least one measure column name")#measure.vars = setdiff(names(x), self$id.vars)
                self$measure.vars = measure.vars
                self$measures = lapply(setNames(nm = self$measure.vars), function(var) eval(substitute(as.measure(var, fun.aggregate = .fun.aggregate, ... = ...),
                                                                                                       list(.fun.aggregate = sub.fun))))
            }
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
            if (self$local) {
                self$data = eval(dtq)
            } else {
                stopifnot(requireNamespace("big.data.table", quietly = TRUE), big.data.table::is.rscl(x) || big.data.table::is.big.data.table(x))
                if (big.data.table::is.rscl(x)) x = big.data.table::as.big.data.table(x)
                x[[expr = dtq, lazy = FALSE, send = TRUE]]
                self$data = x
            }
            invisible(self)
        },
        print = function() {
            fact.data.str = capture.output(str(self$data, give.attr = FALSE))
            cat(c("<fact>", fact.data.str), sep="\n")
            invisible(self)
        },
        build.j = function(measure.vars = self$measure.vars) {
            measure.which = sapply(self$measures, function(x) x$var %in% measure.vars)
            jj = as.call(c(
                list(as.name("list")),
                lapply(self$measures[measure.which], function(x) x$expr())
            ))
            if (isTRUE(getOption("datacube.jj"))) message(paste(deparse(jj, width.cutoff = 500), collapse = "\n"))
            jj
        },
        schema = function() {
            if (self$local) schema.data.table(self$data, empty = c("entity")) else schema.big.data.table(self$data, empty = c("entity"))
        },
        head = function(n = 6L) {
            head(self$data, n)
        },
        subset = function(x, collapse=character(), drop=TRUE) {
            # if (!is.character(drop)) stop("Argument 'drop' to fact$subset must be character type, forein key column names.")
            #if (length(x) && !all(drop %chin% names(x))) stop("All column names provided in 'drop' to fact$subset must exists as fact$id.vars - foreign keys.")
            # stopifnot(by %chin% names(self))
            stopifnot(is.list(x), is.character(collapse), names(x) %chin% self$id.vars, collapse %chin% self$id.vars)
            # must return fact, not a data.table
            r = new.env()
            r$local = self$local
            r$id.vars = self$id.vars
            r$measure.vars = self$measure.vars
            r$measures = self$measures
            if (self$local) {
                anynull = any(sapply(x, is.null))
                dimlens = sapply(x, length)
                II = integer()
                if (!anynull) { # for any NULL there is no need to subset
                    for (dk in names(x)) {
                        ii = self$data[x[dk], nomatch=0L, on=dk, which=TRUE]
                        II = if (dk == names(x)[1L]) ii else intersect(II, ii)
                        if (!length(II)) break # skip further filters when already 0 rows
                    }
                }
                do.drop = (drop && any(dimlens==1L)) || length(collapse)
                r$id.vars = if (do.drop) {
                    setdiff(self$id.vars, c(names(dimlens)[dimlens==1L], collapse))
                } else self$id.vars
                # subset from fact
                r$data = if (length(x)) { # `i` arg
                    if (length(r$id.vars)) self$data[II, eval(self$build.j()), by=c(r$id.vars)] # `by` arg
                    else self$data[II, eval(self$build.j())] # no `by` arg
                } else { # no `i` arg
                    if (length(r$id.vars)) self$data[, eval(self$build.j()), by=c(r$id.vars)] # `by` arg
                    else self$data[, eval(self$build.j())] # no `by` arg
                }
                # sort data
                if (length(r$id.vars)) setkeyv(r$data, r$id.vars)
            }
            if (!self$local) {
                stop("distributed processing for data.cube not yet implemented")
            }
            as.fact.environment(r)
        },
        setindex = function(drop = FALSE) {
            if (self$local) {
                setindexv(self$data, if (!drop) self$id.vars)
            } else {
                stop("TODO")
            }
            invisible(self)
        },
        rollup = function(x,  grouping.sets=list(), ...) {
            stopifnot(is.list(x), sapply(x, is.data.table),
                      #if (length(x)) sapply(x, function(dt) names(dt)[1L]) %chin% names(self$id.vars) else TRUE, 
                      is.list(grouping.sets))
            if (self$local) {
                browser()
                # create new dimension with grouping sets or just query data
                do.create = !!length(grouping.sets)
                
                if (length(x)) { # `i` arg
                    browser()
                    # lapply(grouping.sets, `[[`)
                    
                    self$data[II, eval(self$build.j())]
                } else { # no `i` arg
                    rollup.dims = names(ops)[ops=="+"]
                    cube.dims = names(ops)[ops=="^"]
                    if (length(rollup.dims) && length(cube.dims)) stop("Cannot use both rollup and cube in single call.")
                    groupingcall = substitute(
                        data.table:::`.fun`(self$data, j=.j, by=.by),
                        list(.fun = as.name(if (length(rollup.dims)) "rollup.data.table" else if(length(cube.dims)) "cube.data.table"),
                             .j = self$build.j(),
                             .by = unique(unname(unlist(grouping.sets))))
                    )
                    browser()
                    # - [ ] TODO did not lookup higher attributes YET!
                    eval(groupingcall)
                }
                
            }
            if (!self$local) {
                stop("distributed processing for data.cube not yet implemented")
                # - [ ] must bind by grouping dimension when downloading from nodes
                # - [ ] double check validation of totals re-calculated vs existing from nodes
            }
        },
        # fact$query method to be removed - currently used only in tests-big.data.cube.R and tests-method-query.R - see data.cube#12
        query = function(i, i.dt, by, measure.vars = self$measure.vars) {
            
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
        }
    )
)

#' @title Test if fact class
#' @param x object to tests.
is.fact = function(x) inherits(x, "fact")

str.fact = function(object, ...) {
    print(object$schema())
    invisible()
}

names.fact = function(x) as.character(names(x$data))
length.fact = function(x) as.integer(length(x$data))
dim.fact = function(x) as.integer(dim(x$data))
