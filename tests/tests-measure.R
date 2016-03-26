library(data.table)
library(data.cube)

# basic
m = as.measure(x = "z", label = "score etc.", fun.aggregate = "sum", na.rm = TRUE)
stopifnot(
    is.measure(m),
    # expr
    is.language(m$expr()),
    # print
    identical(capture.output(m$print()), "sum(z, na.rm = TRUE)")
)
# fun.format
m = as.measure(x = "z", label = "score etc.", fun.aggregate = "sum", na.rm = TRUE, fun.format = function(x) sprintf("$%s", x))
stopifnot(
    is.function(m$fun.format),
    m$fun.format(1002.5) == "$1002.5"
)
m = as.measure(x = "z", label = "score etc.", fun.aggregate = "sum", na.rm = TRUE, fun.format = currency.format)
stopifnot(
    is.function(m$fun.format),
    m$fun.format(1002.5) == "$1,002.50"
)

# measure in action ----

# dc
# m = as.measure(x = "z", fun.aggregate = "sum", na.rm = TRUE)
# stopifnot(
#     is.function(m$fun.format),
#     m$fun.format(1002.5) == "$1002.5"
# )
