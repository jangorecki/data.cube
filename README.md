# data.cube [![Build Status](https://gitlab.com/jangorecki/data.cube/badges/master/build.svg)](https://gitlab.com/jangorecki/data.cube/builds)

In-memory *OLAP cubes* R data type. Uses high performance C-implemented [data.table](https://github.com/Rdatatable/data.table) R package.  

# Features

- [x] scalable multidimensional hierarchical `array` alternative
- [x] uses [data.table](https://github.com/Rdatatable/data.table/wiki) under the hood
- [x] use base R `array` query API
  - [x] `[.data.cube` uses base R `[.array` method API for *slice* and *dice* of dimension keys
- [x] extends base R `array` query API
  - [x] *slice* and *dice* on dimension attributes of various levels in hierarchy with `.(time_year = 2011:2013)`
  - [x] aggregate by collapse dimensions with `` `-`() ``, also with pre-filtering
  - [ ] rollup and cube over provided groupings with `` `+`() `` for rollup and `` `^`() `` for cube
- [x] `apply.data.cube` uses base R `apply` like API
- [ ] `rollup` for `data.cube`
  - [ ] bind *grouping* dimension to retain cube normalization and avoid double counting
- [x] for *pivot* use `format`/`as.data.table` with `dcast.data.table` API
- [x] direct access to *data.cube* child classes and attributes
- [ ] data.cube queries processing metadata [logging to database](https://gitlab.com/jangorecki/logR)
- [ ] query optimization
  - [ ] use [blazingly fast](https://jangorecki.github.io/blog/2015-11-23/data.table-index.html) [data.table indices](https://rawgit.com/wiki/Rdatatable/data.table/vignettes/datatable-secondary-indices-and-auto-indexing.html)
  - [ ] use [data.table#1377](https://github.com/Rdatatable/data.table/issues/1377) grouping sets
- [ ] works on sharded distributed engine using [big.data.table](https://gitlab.com/jangorecki/big.data.table)

# Installation

```r
install.packages("data.cube", repos = paste0("https://", c(
    "jangorecki.gitlab.io/data.cube",
    "Rdatatable.github.io/data.table",
    "cran.rstudio.com"
)))
```

# Usage

Read [manual](https://jangorecki.gitlab.io/data.cube/library/data.cube/html/00Index.html) and check [*Subset and aggregate multidimensional data with data.cube*](https://jangorecki.gitlab.io/data.cube/library/data.cube/doc/sub-.data.cube.html) vignette.

## Advanced

### client-server

Running as a services with data.cube can be run using [Rserve: TCP/IP or local sockets](https://github.com/s-u/Rserve), [httpuv: HTTP and WebSocket server](https://github.com/rstudio/httpuv) or [svSocket: R socket server](https://github.com/SciViews/svSocket).  
Parsing [MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions) queries or [XMLA](https://en.wikipedia.org/wiki/XML_for_Analysis) requests would be nice extension but is not on the roadmap currently.  

# Interesting reading

- [Should OLAP databases be denormalized for read performance?](http://stackoverflow.com/q/4394183/2490497)
- [OLAP Operation in R](https://dzone.com/articles/olap-operation-r) + [r-script](https://gist.github.com/jangorecki/4aa6218b6011360338f2)
- [data.table 2E9 rows grouping benchmark](https://github.com/Rdatatable/data.table/wiki/Benchmarks-%3A-Grouping)
- [data.table vs python, big data, MPP, databases](https://github.com/szilard/benchm-databases)

# Contact

`j.gorecki@wit.edu.pl`
