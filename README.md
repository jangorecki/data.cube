
In-memory *OLAP cubes* R data type. Uses high performance C-implemented [data.table](https://github.com/Rdatatable/data.table) R package.  

# Features and examples

- [x] scalable multidimensional `array` alternative, data modeled in *star schema*
- [x] uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- [x] use base R `array` query API
  - [x] `[.cube` uses base R `[.array` method API for *slice* and *dice*, see [tests/tests-sub-.cube.R](tests/tests-sub-.cube.R)
  - [x] `capply`/`aggregate.cube`/`rollup` uses base R `apply` function like API for *rollup*, *drilldown*, see [tests/tests-capply.R](tests/tests-capply.R) and [tests/tests-rollup.R](tests/tests-rollup.R)
- [x] for *pivot* use `format`/`as.data.table` with `dcast.data.table` API, see [tests/tests-format.R](tests/tests-format.R)
- [x] base R `array` API is extended by accepting multiple attributes from dimensions and hierarchies
- [ ] new `[[.cube` method combine and optimize `[.cube` and `capply` into single call with *data.table*-like API, see [tests/tests-sub-sub-.cube.R](tests/tests-sub-sub-.cube.R)
  - [ ] *i* accept same input as `...` argument of `[.cube` wrapped into `.(...)`
  - [ ] *j* accept input like data.table *j* or a function to apply on all measures
  - [ ] *by* acts like a `MARGIN` arg of `apply`, accept input like data.table *by*
- [x] direct access to *cube* class methods and attributes, see `ls.str(x)` on *cube* object
- [ ] logging of queries against the cube
- [x] query optimization
  - [x] uses blazingly fast data.table's *binary search* where possible
- [ ] share dimensions between cubes

Contribution welcome!  

# Installation

Use git clone, once 0.2 will be closed, below R command will work.  

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

# sample array
set.seed(1L)
ar.dimnames = list(color = sort(c("green","yellow","red")), 
                   year = as.character(2011:2015), 
                   status = sort(c("active","inactive","archived","removed")))
ar.dim = sapply(ar.dimnames, length)
ar = array(sample(c(rep(NA, 4), 4:7/2), prod(ar.dim), TRUE), 
           unname(ar.dim),
           ar.dimnames)
print(ar)

cb = as.cube(ar)
print(cb)
str(cb)
all.equal(ar, as.array(cb))
all.equal(dim(ar), dim(cb))
all.equal(dimnames(ar), dimnames(cb))

# slice

arr = ar["green",,]
print(arr)
r = cb["green",]
print(r)
all.equal(arr, as.array(r))

arr = ar["green",,,drop=FALSE]
print(arr)
r = cb["green",,,drop=FALSE]
print(r)
all.equal(arr, as.array(r))

arr = ar["green",,"active"]
r = cb["green",,"active"]
all.equal(arr, as.array(r))

# dice

arr = ar["green",, c("active","archived","inactive")]
r = cb["green",, c("active","archived","inactive")]
all.equal(arr, as.array(r))
as.data.table(r)
as.data.table(r, na.fill = TRUE)
# array-like print using data.table, useful cause as.array doesn't scale
as.data.table(r, na.fill = TRUE, dcast = TRUE, formula = year ~ status)
print(arr)

# apply

format(aggregate(cb, c("year","status"), sum))
format(capply(cb, c("year","status"), sum))

# rollup and drilldown

# granular data with all totals
r = rollup(cb, MARGIN = c("color","year"), FUN = sum)
format(r)

# chose subtotals - drilldown to required levels of aggregates
r = rollup(cb, MARGIN = c("color","year"), INDEX = 1:2, FUN = sum)
format(r)

# pivot
r = cb["green"]
format(r, dcast = TRUE, formula = year ~ status)
```

## Extension to array

```r
library(data.table)
library(data.cube)

X = populate_star(1e5)
lapply(X, sapply, ncol)
lapply(X, sapply, nrow)
cb = as.cube(X)
str(cb)

# slice and dice on dimension hierarchy
cb["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# same as above but more verbose
cb$dims
cb[product = "Mazda RX4",
   customer = .(),
   currency = .(curr_type = "crypto"),
   geography = .(),
   time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]

# apply on dimension hierarchy
format(aggregate(cb, c("time_year","geog_region_name"), sum))
format(capply(cb, c("time_year","geog_region_name"), sum))

# rollup, drilldown and pivot on dimension hierarchy
r = rollup(cb, c("time_year","geog_region_name", "curr_type","prod_gear"), FUN = sum)
print(r) # new dimension *level*
# various levels of aggregates starting from none
format(r[,,,,0L])
format(r[,,,,1L])
format(r[,,,,2L])
format(r[,,,,3L])
format(r[,,,,4L]) # grand total
# be aware of double counting which occurs in rollup, unless you provide scalar integer to INDEX arg of `rollup`.
format(r[,,,,2:4])

# rollup by multi attrs from single dimension will produce (by default) a surrogate key to enforce normalization
r = rollup(cb, c("time_year","time_month"), FUN = sum)
format(r)
# so we may want to use `normalize` and get data.table directly
r = rollup(cb, c("time_year","time_month"), FUN = sum, normalize=FALSE)
print(r)

# pivot by regular dcast.data.table
r = aggregate(cb, c("time_year", "geog_division_name"), FUN = sum)
as.data.table(r, dcast = TRUE, formula = geog_division_name ~ time_year)

# denormalize
cb$denormalize()

# out
X = as.list(cb)
dt = as.data.table(cb) # wraps to cb$denormalize
#ar = as.array(cb) # arrays scales badly, prepare task manager to kill R

# in
#as.cube(ar)
as.cube(X)
dimcolnames = cb$dapply(names)
print(dimcolnames)
as.cube(dt, fact = "sales", dims = dimcolnames)
```

## Advanced

### Normalization

Data in *cube* are normalized into star schema. In case of rollup on attributes from the same hierarchy, the dimension will be wrapped with new surrogate key. Use `normalize=FALSE` to return data.table with subtotals.  

### data.table indexes

User can utilize data.table indexes which dramatically reduce query time.  

```
system.nanotime(filter_with_index(x, col = NULL, i = qi))
#     user    system   elapsed 
#       NA        NA 0.1294823
system.nanotime(filter_with_index(x, col = "biggroup", i = qi))
#Using existing index 'biggroup'
#Starting bmerge ...done in 0 secs
#       user      system     elapsed 
#         NA          NA 0.001833093 
```

Full benchmark script available in [this gist](https://gist.github.com/jangorecki/e381c8783a210a89ae47).  
Example usage of data.table index on cube object.  

```r
library(data.cube)

cb = as.cube(populate_star(1e5))

# use prod(dim()) attribute to see how long array would need to be for single measure
prod(dim(cb))

# binary search, index
op = options("datatable.verbose" = TRUE, "datatable.auto.index" = TRUE)
cb["Mazda RX4", c("1","6"), c("AZN","SEK")] # binary search
cb["Mazda RX4",, c("AZN","SEK")] # binary search + vector scan/index
cb["Mazda RX4",, .(curr_type = c("fiat","crypto"))] # lookup to currency hierarchy
set2keyv(cb$env$dims$time, "time_year")
cb["Mazda RX4",, .(curr_type = c("fiat","crypto")),, .(time_year = 2011:2012)] # use index
options(op)
```

### Architecture

Design concept is very simple.  
Cube is [R6](https://github.com/wch/R6) class object, which is enhanced R environment object.  
A cube class keeps another plain R environment container to store all tables.  
Tables are stored as [data.table](https://github.com/Rdatatable/data.table) class object, which is enhanced R data.frame object.  
All of the cube attributes are dynamic, static part is only *star schema* modeled multidimensional data.  
Logic of cubes can be isolated from the data, they can also run as a service.  

#### client-server

Another package development is planned to wrap services upon data.cube.  
It would allow to use `[.cube` and `[[.cube` methods via [Rserve: TCP/IP or local sockets](https://github.com/s-u/Rserve) or [httpuv: HTTP and WebSocket server](https://github.com/rstudio/httpuv).  
Basic parsers of [MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions) queries and [XMLA](https://en.wikipedia.org/wiki/XML_for_Analysis) requests.  
It could potentially utilize `Rserve` for parallel processing on distributed data partitions, see [this gist](https://gist.github.com/jangorecki/ecccfa5471a633acad17).  

# Interesting reading

- [Should OLAP databases be denormalized for read performance?](http://stackoverflow.com/q/4394183/2490497)
- [OLAP Operation in R](https://dzone.com/articles/olap-operation-r) + [r-script](https://gist.github.com/jangorecki/4aa6218b6011360338f2)
- [data.table 2E9 rows grouping benchmark](https://github.com/Rdatatable/data.table/wiki/Benchmarks-%3A-Grouping)
- [data.table vs python, big data, MPP, databases](https://github.com/szilard/benchm-databases)

# Contact

`j.gorecki@wit.edu.pl`
