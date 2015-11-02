
In-memory *OLAP cubes* R data type. Uses high performance C-implemented [data.table](https://github.com/Rdatatable/data.table) R package.  

# Features and examples

- [x] scalable multidimensional `array` alternative, data normalized to star schema
- [x] uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- [x] base R `array` like API, see [olap-operation-r](https://dzone.com/articles/olap-operation-r) and [r-script](https://gist.github.com/jangorecki/4aa6218b6011360338f2)
  - [x] `[.cube` uses base R `[.array` method API for *slice* and *dice*, see [tests/subset_cube.R](tests/subset_cube.R)
  - [ ] `capply`/`aggregate.cube` uses base R `apply` function API for *rollup*, *drilldown* and *pivot*
- [x] base R `array` API is extended by accepting named list instead of vectors, see [tests/subset_cube.R](tests/subset_cube.R)
  - [x] slice and dice on multiple attributes from dimensions and hierarchies
  - [ ] rollup, drilldown and pivot on multiple attributes from dimensions and hierarchies
  - [x] NULL list elements in `[` to lookup column without filter
  - [x] NULL to `[` to lookup all the columns from dimension
- [ ] `query` combines `[.cube` and `capply` into single logical query call
- [x] direct access to *cube* class methods and attributes, use `ls.str(x)` / `ls.str(x$db)` on *cube* object
- [ ] logging of all queries
- [x] query optimization
  - [x] uses blazingly fast data.table's *binary search* where possible
- [x] share dimensions between cubes

Contribution welcome!  

# Installation

```r
install.packages("data.cube", repos = paste0("https://",
    c("jangorecki.github.io/data.cube","cran.rstudio.com")
))
```

# Usage

## Basics

```r
library(data.cube)
set.seed(1L)

dimnames = list(color = sort(c("green","yellow","red")), 
                year = 2011:2015, 
                status = sort(c("active","inactive","archived","removed")))
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(sapply(dimnames, length)), TRUE), 
           sapply(dimnames, length), 
           dimnames)
print(ar)

cb = as.cube(ar)
print(cb)
ar2 = as.array(cb)
all.equal(ar, ar2)

# slice and dice using array syntax
ar["green",, c("active","inactive")]
cb["green",, c("active","inactive")]

# rollup, drilldown and pivot using array syntax
# apply()
# capply()
```

## Extensions to array

```r
library(data.cube)
set.seed(1L)

cb = as.cube(populate_star(1e4))

# slice and dice on dimension hierarchy
cb

# rollup, drilldown and pivot on dimension hierarchy
# capply

# denormalize
cb$denormalize()

# out
sr = as.list(cb)
dt = as.data.table(cb)
ar = as.array(cb)

# in
as.cube(sr)
dim_cols = lapply(setNames(cb$dims, cb$dims), function(dim) names(cb$db[[dim]]))
as.cube(dt, fact = "sales", dims = dim_cols)
```

## Advanced

```r
library(data.cube)
set.seed(1L)

cb = as.cube(populate_star(1e4))

# binary search, index
options("datatable.verbose" = TRUE)
cb["Mazda RX4", c("1","6"), c("AZN","SEK")] # binary search
cb["Mazda RX4",, c("AZN","SEK")] # binary search + vector scan/index
cb["Mazda RX4",, .(curr_type = c("fiat","crypto"))] # lookup to currency hierarchy
# set2keyv(cb$db$time, "time_year") # uncomment after data.table#1422
# cb["Mazda RX4",, .(curr_type = c("fiat","crypto"))]
options("datatable.verbose" = FALSE)

# shared dimensions

```


# Contact

`j.gorecki@wit.edu.pl`
