library(data.table)
library(data.cube)

# level ----

dt = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA)
lvl = level$new(x = dt, key = "a", properties = c("b","d"))
stopifnot(
    inherits(lvl, "level"),
    identical(lvl$key, "a"),
    identical(lvl$properties, c("b","d")),
    identical(dim(lvl$data), c(6L, 3L))
)

# hierarchy ----

hrh1 = hierarchy$new(levels = list("b" = character(), "a" = c("b")))
hrh2 = hierarchy$new(levels = list("d" = character(), "a" = c("d")))
stopifnot(
    is.list(hrh1$levels),
    length(hrh1$levels) == 2L,
    identical(unname(sapply(hrh1$levels, length)), 0:1)
)

# dimension ----

dt = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA)
ddim = dimension$new(
    x = dt,
    key = "a",
    hierarchies = list(
        list("b" = character(), "a" = c("b")),
        list("d" = character(), "a" = c("d"))
    )
)
stopifnot(
    identical(dim(ddim$data), c(6L, 3L)),
    identical(ddim$key, "a"),
    length(ddim$hierarchies) == 2L,
    sapply(ddim$hierarchies, function(x) length(x$levels)) == 2L,
    c(sapply(ddim$hierarchies, function(x) names(x$levels))) == c("b","a","d","a")
)

# time dimension
X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)
time = dimension$new(X$dims$time,
                     key = "time_date",
                     hierarchies = X$hierarchies$time)
stopifnot(
    inherits(time, "dimension"),
    names(time$hierarchies) == c("monthly","weekly"),
    time$hierarchies$monthly$key == "time_date",
    # all leafs in levels are normalized data.tables
    unlist(lapply(time$levels, function(lvl) is.data.table(lvl$data)))
)
rm(X)

# measure ----

m = measure$new(x = "z", label = "score etc.", fun.aggregate = "sum", na.rm = TRUE)
stopifnot(
    is.measure(m)
)

# fact ----

dt = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = 1:12*sin(1:12))
ff = fact$new(x = dt,
              id.vars = c("a","b","d"),
              measure.vars = "z")
stopifnot(
    identical(dim(ff$data), c(6L, 4L))
)

# data.cube ----

X = populate_star(N = 1e3, surrogate.keys = FALSE, hierarchies = TRUE)

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
