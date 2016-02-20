library(data.table)
library(data.cube)

# initialize ----

dt = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA)
ddim = dimension$new(
    x = dt,
    id.vars = "a",
    hierarchies = list(
        list("b" = character(), "a" = c("b")),
        list("d" = character(), "a" = c("d"))
    )
)
stopifnot(
    identical(dim(ddim$data), c(6L, 3L)),
    identical(ddim$id.vars, "a"),
    length(ddim$hierarchies) == 2L,
    sapply(ddim$hierarchies, function(x) length(x$levels)) == 2L,
    c(sapply(ddim$hierarchies, function(x) names(x$levels))) == c("b","a","d","a")
)

# time dimension ----

X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)
time = dimension$new(X$dims$time,
                     id.vars = "time_date",
                     hierarchies = X$hierarchies$time)
stopifnot(
    inherits(time, "dimension"),
    names(time$hierarchies) == c("monthly","weekly"),
    time$id.vars == "time_date",
    # all leafs in levels are normalized data.tables
    unlist(lapply(time$levels, function(lvl) is.data.table(lvl$data)))
)

# batch dimensions ----

X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)
dims = lapply(setNames(seq_along(X$dims), names(X$dims)), function(i){
    dimension$new(X$dims[[i]],
                  id.vars = key(X$dims[[i]]),
                  hierarchies = X$hierarchies[[i]])
})

stopifnot(
    sapply(dims, inherits, "dimension"),
    # dimension fields
    sapply(dims, function(x) is.character(x$fields) & length(x$fields) > 0L)
)

# as.data.table.dimension ----

ld = lapply(dims, as.data.table)
stopifnot(
    sapply(ld, is.data.table),
    all.equal(sapply(ld, ncol), structure(c(5L, 4L, 2L, 4L, 7L), .Names = c("product", "customer", "currency", "geography", "time")))
)
