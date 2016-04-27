
In-memory *OLAP cubes* R data type. Uses high performance C-implemented [data.table](https://github.com/Rdatatable/data.table) R package.  

# Features and examples

- [x] scalable multidimensional `array` alternative
- [x] uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- [x] use base R `array` query API
  - [x] `[.data.cube` uses base R `[.array` method API for *slice* and *dice*
  - [x] extended for aggregation subsetting with `[.array`
  - [x] `apply.data.cube` uses base R `apply` function like API
- [x] aggregate in `[.data.cube` using `+(.)`
- [ ] `rollup` for `data.cube`
- [x] for *pivot* use `format`/`as.data.table` with `dcast.data.table` API
- [x] base R `array` API is extended by accepting multiple attributes from dimensions and hierarchies
- [x] direct access to *data.cube* class methods and attributes, see `ls.str(x)` on *data.cube* object
- [ ] logging of queries against data.cube
- [ ] query optimization
  - [ ] can uses blazingly fast data.table *indexes*
- [ ] works on sharded engine using [big.data.table](https://gitlab.com/jangorecki/big.data.table)

Contribution welcome!  

# Installation

```r
install.packages("data.cube", repos = paste0("https://", c("jangorecki.gitlab.io/data.cube","Rdatatable.github.io/data.table","cran.rstudio.com"))
)
```

# Usage

Check following vignettes:  

- `data.cube` class
  - [Subset and aggregate multidimensional data with data.cube](https://jangorecki.gitlab.io/data.cube/library/data.cube/doc/sub-.data.cube.html)

- old basic `cube` class
  - [Subset multidimensional data](https://jangorecki.gitlab.io/data.cube/library/data.cube/doc/sub-.cube.html)

## Basics

```r
set.seed(1)
# array
ar = array(rnorm(8,10,5), rep(2,3), 
           dimnames = list(color = c("green","red"), 
                           year = c("2014","2015"), 
                           country = c("IN","UK"))) # sorted
# cube normalized to star schema just on natural keys
dc = as.data.cube(ar)

# slice
ar["green","2015",]
dc["green","2015"]
format(dc["green","2015"])

# dice
ar[c("green","red"),c("2014","2015"),]
dc[c("green","red"),c("2014","2015")]
format(dc[c("green","red"),c("2014","2015")])

# exact tabular representation of array is just a formatting on the cube
ar["green",c("2014","2015"),]
format(dc["green",c("2014","2015")], 
       dcast = TRUE, 
       formula = year ~ country)
ar[,"2015",c("UK","IN")]
format(dc[,"2015",c("UK","IN")], 
       dcast = TRUE, 
       formula = color ~ country) # sorted dimensions levels
```

## Dimension hierarchies and attributes, aggregation

Build data.cube from set of tables defined with star schema, single fact table and multiple dimensions.

```r
library(data.table)
library(data.cube)

X = populate_star(1e5)
lapply(X, sapply, ncol)
lapply(X, sapply, nrow)
dc = as.data.cube(X)
str(dc) # model live description

# slice and dice on dimension hierarchy
dc["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# same as above but more verbose
names(dc$dimensions)
dc[product = "Mazda RX4",
   customer = .(),
   currency = .(curr_type = "crypto"),
   geography = .(),
   time = .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]

# aggregate by droppping dimension with just `+(.)` symbol, group by customer and currency
dc[product = +(.),
   customer = .(),
   currency = .(curr_type="crypto"),
   geography = +(.),
   time = +(.)]
```

### old `cube` class with more methods

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
# aggregates does not gets surrogate keys so we may want to use `normalize=FALSE` and get data.table directly
r = rollup(cb, c("time_year","time_month"), FUN = sum, normalize=FALSE)
print(r)

# pivot by regular dcast.data.table
r = aggregate(cb, c("time_year", "geog_division_name"), FUN = sum)
as.data.table(r, dcast = TRUE, formula = geog_division_name ~ time_year)

# denormalize
cb$denormalize()
```

## Advanced

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

### client-server

Running as a services with data.cube could be run [Rserve: TCP/IP or local sockets](https://github.com/s-u/Rserve), [httpuv: HTTP and WebSocket server](https://github.com/rstudio/httpuv) or [svSocket](https://github.com/SciViews/svSocket).  
[MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions) queries parser skills welcome, could be wrapped into simple [XMLA](https://en.wikipedia.org/wiki/XML_for_Analysis) requests.  

# Interesting reading

- [Should OLAP databases be denormalized for read performance?](http://stackoverflow.com/q/4394183/2490497)
- [OLAP Operation in R](https://dzone.com/articles/olap-operation-r) + [r-script](https://gist.github.com/jangorecki/4aa6218b6011360338f2)
- [data.table 2E9 rows grouping benchmark](https://github.com/Rdatatable/data.table/wiki/Benchmarks-%3A-Grouping)
- [data.table vs python, big data, MPP, databases](https://github.com/szilard/benchm-databases)

# Contact

`j.gorecki@wit.edu.pl`
