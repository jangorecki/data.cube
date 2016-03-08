library(data.table)
library(data.cube)

# initialize ----

m = as.measure(x = "z", label = "score etc.", fun.aggregate = "sum", na.rm = TRUE)
stopifnot(is.measure(m))

# expr ----

stopifnot(
    is.language(m$expr())
)

# print ----

stopifnot(
    capture.output(m$print()) == "sum(z, na.rm = TRUE)"
)

# fun.format ----

m = as.measure(x = "z", label = "score etc.", fun.aggregate = "sum", na.rm = TRUE, fun.format = function(x) sprintf("$ %s", x))
stopifnot(
    is.function(m$fun.format),
    m$fun.format(12.5) == "$ 12.5"
)

# tests status ------------------------------------------------------------

invisible(FALSE)
