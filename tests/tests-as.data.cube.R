library(data.table)
library(data.cube)

X = populate_star(N = 1e5, surrogate.keys = FALSE)
time.hierarchies = list(
    "monthly" = list(
        "time_year" = character(),
        "time_quarter" = c("time_quarter_name"),
        "time_month" = c("time_month_name"),
        "time_date" = c("time_month","time_quarter","time_year")
    ),
    "weekly" = list(
        "time_year" = character(),
        "time_week" = character(),
        "time_date" = c("time_week","time_year")
    )
)
geog.hierarchies = list(list(
    "geog_region_name" = character(),
    "geog_division_name" = character(),
    "geog_abb" = c("geog_name","geog_division_name","geog_region_name")
))
dimensions = list(
    time = as.dimension(X$dims$time, key = "time_date", hierarchies = time.hierarchies),
    geography = as.dimension(X$dims$geography, key = "geog_abb", hierarchies = geog.hierarchies)
)

facts = as.fact(X$fact$sales,
                id.vars = c("geog_abb","time_date"),
                measure.vars = c("amount","value"),
                fun.aggregate = "sum",
                na.rm = TRUE)
dc = as.data.cube(facts, dimensions)

stopifnot(
    is.data.cube(dc)
)
