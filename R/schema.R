# stats collector for data.table
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
