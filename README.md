
In-memory *OLAP cubes* R data type. Uses high performance C-implemented [data.table](https://github.com/Rdatatable/data.table) R package.  

# Features and examples

- [x] scalable multidimensional `array` alternative, data modeled in *star schema*
- [x] uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- [x] use base R `array` query API, see [olap-operation-r](https://dzone.com/articles/olap-operation-r) and [r-script](https://gist.github.com/jangorecki/4aa6218b6011360338f2)
  - [x] `[.cube` uses base R `[.array` method API for *slice* and *dice*, see [tests/tests-sub-.cube.R](tests/tests-sub-.cube.R)
  - [ ] `capply`/`aggregate.cube` uses base R `apply` function API for *rollup*, *drilldown* and *pivot*, see [tests/tests-capply.R](tests/tests-capply.R)
- [x] base R `array` API is extended by accepting named list instead of vectors
  - [ ] slice and dice on multiple attributes from dimensions and hierarchies, see [tests/tests-sub-.cube.R](tests/tests-sub-.cube.R)
  - [ ] rollup, drilldown and pivot on multiple attributes from dimensions and hierarchies, see [tests/tests-capply.R](tests/tests-capply.R)
- [ ] new `[[.cube` method combine and optimize `[.cube` and `capply` into single call with *data.table*-like API, see [tests/tests-sub-sub-.cube.R](tests/tests-sub-sub-.cube.R)
  - [ ] *i* accept same input as `...` argument of `[.cube` wrapped into `.(...)`
  - [ ] *j* instead `FUN` arg of `apply`, accept input like data.table *j* or a function to apply on all measures
  - [ ] *by* acts like a `MARGIN` arg of `apply`, accept input like data.table *by*
- [x] direct access to *cube* class methods and attributes, see `ls.str(x)` on *cube* object
- [ ] logging of queries against the cube
- [x] query optimization
  - [x] uses blazingly fast data.table's *binary search* where possible
- [ ] share dimensions between cubes

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
library(data.table)
library(data.cube)
set.seed(1L)

dimnames = list(color = sort(c("green","yellow","red")), 
                year = as.character(2011:2015), 
                status = sort(c("active","inactive","archived","removed")))
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(sapply(dimnames, length)), TRUE), 
           unname(sapply(dimnames, length)),
           dimnames)
print(ar)

cb = as.cube(ar)
print(cb)
str(cb)
all.equal(ar, as.array(cb))

# slice and dice using array syntax

ar["green","2015","active"]
r = cb["green","2015","active"]
print(r)
as.array(r)
as.data.table(r)
as.list(r)

ar["green",, c("active","inactive")]
r = cb["green",, c("active","inactive")]
as.array(r)
as.data.table(r)
as.data.table(r, na.fill = TRUE)
as.data.table(r, na.fill = TRUE, dcast = TRUE, formula = year ~ status)

# rollup, drilldown and pivot using array syntax
# apply()
# capply()
```

## Extensions to array

```r
library(data.table)
library(data.cube)
set.seed(1L)

X = populate_star(1e5)
lapply(X, sapply, ncol)
lapply(X, sapply, nrow)
cb = as.cube(X)
str(cb)

# slice and dice on dimension hierarchy
cb["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
cb$dims
cb[product = "Mazda RX4",
   customer = .(),
   currency = .(curr_type = "crypto"),
   geography = .(),
   time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]

# rollup, drilldown and pivot on dimension hierarchy
# capply

# denormalize
cb$denormalize()

# out
X = as.list(cb)
dt = as.data.table(cb) # wraps to cb$denormalize
#ar = as.array(cb) # arrays scales bad in memory, prepare to kill R session

# in
#as.cube(ar)
as.cube(X)
dimcolnames = cb$dapply(names)
print(dimcolnames)
as.cube(dt, fact = "sales", dims = dimcolnames)
```

## Advanced

```r
library(data.cube)
set.seed(1L)

cb = as.cube(populate_star(1e5))

# use dim attribute to see how long array would need to be for single measure
prod(dim(cb))

# binary search, index
options("datatable.verbose" = TRUE)
cb["Mazda RX4", c("1","6"), c("AZN","SEK")] # binary search
cb["Mazda RX4",, c("AZN","SEK")] # binary search + vector scan/index
cb["Mazda RX4",, .(curr_type = c("fiat","crypto"))] # lookup to currency hierarchy
set2keyv(cb$env$dims$time, "time_year")
cb["Mazda RX4",, .(curr_type = c("fiat","crypto")),, .(time_year = 2011:2012)]
options("datatable.verbose" = FALSE)
```

# Contact

`j.gorecki@wit.edu.pl`
