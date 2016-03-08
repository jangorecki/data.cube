library(data.table)
library(data.cube)

# initialize ----

X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)
dims = lapply(setNames(seq_along(X$dims), names(X$dims)), function(i){
    as.dimension(X$dims[[i]],
                 key = key(X$dims[[i]]),
                 hierarchies = X$hierarchies[[i]])
})
ff = fact$new(x = X$fact$sales,
              id.vars = key(X$fact$sales),
              measure.vars = c("amount","value"),
              fun.aggregate = "sum",
              na.rm = TRUE)
dc = as.data.cube(ff, dims)

stopifnot(
    is.data.table(dc$fact$data),
    # Normalization
    identical(dim(dc$dimensions$geography$levels$geog_abb$data), c(50L, 4L)),
    identical(dim(dc$dimensions$geography$levels$geog_region_name$data), c(4L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_month$data), c(12L, 2L)),
    identical(dim(dc$dimensions$time$levels$time_year$data), c(5L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_date$data), c(1826L, 6L))
)

# print ----

out = capture.output(print(dc))
stopifnot(
    out[1L] == "<data.cube>",
    out[2L] == "fact:",
    out[4L] == "dimensions:",
    length(out) == 10L
)

# str ----

r = capture.output(str(dc))
stopifnot(
    all.equal(r, capture.output(dc$schema()))
)

# dimnames ----

r = dimnames(dc)
stopifnot(
    is.list(r),
    identical(names(r), c("product","customer","currency","geography","time")),
    !sapply(r, function(x) anyDuplicated(r))
)

# schema ----

dict = dc$schema()
stopifnot(
    nrow(dict) == 26L,
    c("type","name","entity","nrow","ncol","mb","address","sorted") %in% names(dict),
    dict[type=="fact", .N] == 1L
)

# head ----

r = dc$head()
stopifnot(
    identical(names(r), c("fact","dimensions")),
    is.data.table(r$fact),
    is.list(r$dimensions),
    identical(names(r$dimensions), c("product", "customer", "currency", "geography", "time")),
    sapply(lapply(r$dimensions, `[[`, "base"), nrow) == 6L,
    unlist(lapply(r$dimensions, function(x) sapply(x$levels, is.data.table)))
)

# subset ----



# d = dc$subset(.(prod_name = "Mazda RX4", prod_gear = c(3L, 4L)),, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter = c(1L,2L), time_quarter_name = c("Q1","Q2")))
# 
# stopifnot(all.equal(
#     cb[.("Mazda RX4", prod_gear = c(3L, 4L)),, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))],
#     cb[product = "Mazda RX4",
#        customer = .(),
#        currency = .(curr_type = "crypto"),
#        geography = .(),
#        time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# ))


