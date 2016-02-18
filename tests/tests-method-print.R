library(data.table)
library(data.cube)

X = populate_star(N = 1e5, surrogate.keys = FALSE, hierarchies = TRUE)

X$dims = X$dims[names(X$dims) %in% c("geography","time")]
X$hierarchies = X$hierarchies[names(X$hierarchies) %in% c("geography","time")]
dims = lapply(setNames(seq_along(X$dims), names(X$dims)), function(i){
    dimension$new(X$dims[[i]],
                  key = key(X$dims[[i]]),
                  hierarchies = X$hierarchies[[i]])
})
ff = fact$new(x = X$fact$sales,
              id.vars = c("geog_abb","time_date"),
              measure.vars = c("amount","value"),
              fun.aggregate = "sum",
              na.rm = TRUE)
dc = data.cube$new(fact = ff, dimensions = dims)

# data.cube$print ----

out = capture.output(print(dc))
stopifnot(
    out[1L] == "<data.cube>",
    out[2L] == "fact:",
    out[4L] == "dimensions:",
    length(out) == 7L
)