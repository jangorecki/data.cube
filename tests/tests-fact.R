library(data.table)
library(data.cube)

# initialize ----

dt = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = 1:12*sin(1:12))
ff = as.fact(x = dt,
             id.vars = c("a","b","d"),
             measure.vars = "z")
stopifnot(
    identical(dim(ff$data), c(6L, 4L))
)
