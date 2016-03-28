# stats collector for data.table and big.data.table
schema.big.data.table = function(x, empty) {
    if (requireNamespace("big.data.table", quietly = TRUE)) {
        rscl = attr(x, "rscl")
        dm = dim(x)
        mb = sum(big.data.table::rscl.eval(rscl, as.numeric(object.size(x))/(1024^2), simplify = TRUE))
        adr = NA_character_
        ky = big.data.table::rscl.eval(rscl[1L], if(haskey(x)) paste(key(x), collapse=", ") else NA_character_, simplify = TRUE)
        dt = data.table(nrow = dm[1L], ncol = dm[2L], mb = mb, address = adr, sorted = ky)
        nn = copy(names(dt))
        if (!missing(empty)) setcolorder(dt[, c(empty) := NA][], unique(c(empty, nn)))
        dt
    } else stop("schema.big.data.table can be only used with `big.data.table` package which is not installed.")
}
schema.data.table = function(x, empty) {
    dm = dim(x)
    mb = as.numeric(object.size(x))/(1024^2)
    adr = as.character(address(x))[1L]
    ky = if(haskey(x)) paste(key(x), collapse=", ") else NA_character_
    dt = data.table(nrow = dm[1L], ncol = dm[2L], mb = mb, address = adr, sorted = ky)
    nn = copy(names(dt))
    if (!missing(empty)) setcolorder(dt[, c(empty) := NA][], unique(c(empty, nn)))
    dt
}
