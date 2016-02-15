library(data.table)
library(data.cube)

# level ----

lvl = level$new(x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA), key = "a", properties = c("b","d"))
stopifnot(
    inherits(lvl, "level"),
    identical(lvl$key, "a"),
    identical(lvl$properties, c("b","d")),
    identical(dim(lvl$data), c(6L, 3L))
)

# hierarchy ----

hrh1 = hierarchy$new(x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA), key = "a", levels = list("b" = character(), "a" = c("b")))
hrh2 = hierarchy$new(x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA), key = "a", levels = list("d" = character(), "a" = c("d")))
stopifnot(TRUE) # TO DO

# dimension ----

dim = dimension$new(
    x = data.table(a = rep(1:6,2), b = letters[1:3], d = letters[1:2], z = NA),
    key = "a",
    hierarchies = list(
        list("b" = character(), "a" = c("b")),
        list("d" = character(), "a" = c("d"))
    )
)
stopifnot(
    identical(base::dim(dim$data), c(6L, 3L)),
    identical(dim$key, "a"),
    length(dim$hierarchies) == 2L,
    sapply(dim$hierarchies, function(x) length(x$levels)) == 2L,
    c(sapply(dim$hierarchies, function(x) names(x$levels))) == c("b","a","d","a")
)

# time dimension
X = data.cube::populate_star(N = 1e5, surrogate.keys = FALSE)
X$dims$time[, time_week := week(time_date)]
time = dimension$new(X$dims$time,
                     key = "time_date",
                     hierarchies = list(
                         "monthly" = list(
                             "time_month" = c("time_month_name"),
                             "time_quarter" = c("time_quarter_name"),
                             "time_year" = character(),
                             "time_date" = c("time_month","time_quarter","time_year")
                         ),
                         "weekly" = list(
                             "time_week" = character(),
                             "time_year" = character(),
                             "time_date" = c("time_week","time_year")
                         )
                     ))
stopifnot(
    inherits(time, "dimension"),
    names(time$hierarchies) == c("monthly","weekly"),
    time$hierarchies$monthly$key == "time_date",
    # all leafs in levels are normalized data.tables
    unlist(lapply(time$hierarchies, function(hrh) lapply(hrh$levels, function(lvl) is.data.table(lvl$data))))
)
sprintf("size of all tables in 'time' dimension: %.3f MB.", sum(unlist(lapply(time$hierarchies, function(hrh) lapply(hrh$levels, function(lvl) as.numeric(object.size(lvl$data))))))/(1024L^2L))
rm(X)

# fact ----

# cube ----

# data.cube ----

