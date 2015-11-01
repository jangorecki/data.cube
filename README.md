
In-memory *OLAP cubes* R data type. Uses high performance C-implemented [data.table](https://github.com/Rdatatable/data.table) R package.  

# Features and examples

- [x] scalable multidimensional `array` alternative, data normalized to star schema
- [x] uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- [x] base R `array` like API, see [olap-operation-r](https://dzone.com/articles/olap-operation-r) and [r-script](https://gist.github.com/jangorecki/4aa6218b6011360338f2)
  - [x] `[.cube` uses base R `[.array` method API for *slice* and *dice*, see [tests/subset_cube.R](tests/subset_cube.R)
  - [ ] `capply`/`aggregate.cube` uses base R `apply` function API for *rollup*, *drilldown* and *pivot*
- [x] base R `array` API is extended by accepting named list instead of vectors, you can use known `.()` wrapper, see [tests/subset_cube.R](tests/subset_cube.R)
  - [x] slice and dice on multiple attributes from dimensions hierarchies
  - [ ] rollup, drilldown and pivot on multiple attributes from dimensions hierarchies
  - [x] NULL list elements in `[` to lookup column without filter
  - [x] NULL to `[` to lookup all the columns from dimension
- [ ] `query` combines `[.cube` and `capply` into single logical query call
- [x] direct access to *cube* class methods and attributes, use `ls.str(x)` / `ls.str(x$db)` on your cube object
- [ ] logging of all queries
- [x] query optimization
  - [x] uses blazingly fast data.table's *binary search* on fact and dimension tables
- [x] share dimensions between cubes

Contribution welcome!  

# Installation

```r
install.packages("data.cube", repos = paste0("https://",
    c("jangorecki.github.io/data.cube","cran.rstudio.com")
))
```

# Usage

```r
library(data.cube)
# array-data.cube example
```
