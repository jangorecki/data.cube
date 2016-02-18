library(data.table)
library(data.cube)

X = populate_star(N = 1e5, surrogate.keys = FALSE, hierarchies = TRUE)

dims = lapply(setNames(seq_along(X$dims), names(X$dims)), function(i){
    dimension$new(X$dims[[i]],
                  key = key(X$dims[[i]]),
                  hierarchies = X$hierarchies[[i]])
})
ff = fact$new(x = X$fact$sales,
              id.vars = sapply(dims, `[[`, "key"),
              measure.vars = c("amount","value"),
              fun.aggregate = "sum",
              na.rm = TRUE)
dc = data.cube$new(fact = ff, dimensions = dims)
stopifnot(
    is.data.table(dc$fact$data),
    # Normalization
    identical(dim(dc$dimensions$geography$levels$geog_abb$data), c(50L, 4L)),
    identical(dim(dc$dimensions$geography$levels$geog_region_name$data), c(4L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_month$data), c(12L, 2L)),
    identical(dim(dc$dimensions$time$levels$time_year$data), c(5L, 1L)),
    identical(dim(dc$dimensions$time$levels$time_date$data), c(1826L, 5L))
)

# print ----

out = capture.output(print(dc))
stopifnot(
    out[1L] == "<data.cube>",
    out[2L] == "fact:",
    out[4L] == "dimensions:",
    length(out) == 10L
)

# schema ----

dict = dc$schema()
stopifnot(
    nrow(dict) == 25L,
    c("type","name","entity","nrow","ncol","mb","address","sorted") %in% names(dict),
    dict[type=="fact", .N] == 1L
)
