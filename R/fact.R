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
            self$id.vars = unname(id.vars)
            # - [x] `fact$new` creates measures, or use provided in `measures` argument
            if(!missing(measures)){
                self$measures = measures
                self$measure.vars = names(self$measures)
            } else {
                if(!length(measure.vars)) stop("You need to provide at least one measure column name")#measure.vars = setdiff(names(x), self$id.vars)
                self$measure.vars = measure.vars
                self$measures = lapply(setNames(nm = self$measure.vars), function(var) as.measure(var, fun.aggregate = fun.aggregate, ... = ...))
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
        subset = function(x, drop = TRUE){
            # must return fact, not a data.table
            r = new.env()
            r$local = self$local
            r$id.vars = self$id.vars
            r$measure.vars = self$measure.vars
            r$measures = self$measures
            if (self$local) {
                i = 0L
                for (dimk in names(x)) {
                    i = i + 1L
                    if (i == 1L) {
                        r$data = self$data[x[dimk], nomatch=0L, on=dimk]
                    } else {
                        r$data = r$data[x[dimk], nomatch=0L, on=dimk]
                    }
                }
                if (drop) {
                    drop.dims = sapply(x, length) == 1L
                    drop.cols = names(drop.dims)[drop.dims]
                    sapply(drop.cols, function(col) {
                        set(r$data, j = col, value = NULL)
                        TRUE
                    })
                    r$id.vars = r$id.vars[!r$id.vars %chin% drop.cols]
                }
                setkeyv(r$data, r$id.vars)
            } else {
                stop("not yet implemented")
            }
            as.fact(r)
        },
        index = function(.log = getOption("datacube.log")){
            NULL
        }
    )
)

#' @title Test if fact class
#' @param x object to tests.
is.fact = function(x) inherits(x, "fact")