
In-memory *OLAP cubes* R data type. Uses high performance C-implemented [data.table](https://github.com/Rdatatable/data.table) R package.  

# Features and examples

- [x] scalable multidimensional `array` alternative
- [x] uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- [x] base R `array` like API, see [olap-operation-r](https://dzone.com/articles/olap-operation-r) and [r-script](https://gist.github.com/jangorecki/4aa6218b6011360338f2)
  - [x] `[.cube` uses base R `[.array` method API for *slice* and *dice*, see [tests/subset_cube.R](tests/subset_cube.R)
  - [ ] `capply`/`aggregate.cube` uses base R `apply` function API for *rollup*, *drilldown* and *pivot*
- [x] base R `array` API is extended by accepting named list instead of vectors, you can use known `.()` wrapper, see [tests/subset_cube.R](tests/subset_cube.R)
  - [x] slice and dice on multiple attributes from dimensions hierarchies
  - [ ] rollup, drilldown and pivot on multiple attributes from dimensions hierarchies
  - [x] NULL list elements to lookup column without filter
  - [ ] NULL to `[` to will lookup all the columns from dimension
- [ ] `query` combines `[.cube` and `capply` into single logical query call
- [x] direct access to *cube* class methods and attributes, use `ls.str()` on your cube object
- [ ] logging of all queries
- [x] query optimization
  - [x] uses blazingly fast data.table's *binary search* on fact and dimension tables
- [ ] share dimensions between cubes

Contribution welcome!  

# Installation

```r
install.packages("data.cube", repos = paste0("https://",
    c("jangorecki.github.io/data.cube","cran.rstudio.com")
))
```

# Usage


Example dataset [RStudio CRAN logs of packages downloads](http://cran-logs.rstudio.com) from last 3 days aggregated into 3 artificial dimensions:  
- *time* dimension, natural key: `date`
- *environment* dimension, client environment setup, natural keys: `r_version`, `arch`, `os`
- *package version* dimension, natural key: `package`, `version`

```r
library(data.cube)

# download logs from last 3 days and build cube
source(system.file("cubes","cranlogs.R", package="data.cube"))
print(cranlogs)

# slice dice
pkgs = c("RSQLite", "RPostgreSQL", "RMySQL", "ROracle", "RODBC", "RJDBC", "RSQLServer")
cranlogs[,,.(package = pkgs)]
cranlogs[NULL,,.(package = pkgs)]
lapply(cranlogs$db, print)
# rollup
by = c("year","month")
capply(cranlogs, by, sum)
```
