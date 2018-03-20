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
- [ ] query optimization
  - [ ] use [blazingly fast](https://jangorecki.github.io/blog/2015-11-23/data.table-index.html) [data.table indices](https://rawgit.com/wiki/Rdatatable/data.table/vignettes/datatable-secondary-indices-and-auto-indexing.html)
  - [ ] use [data.table#1377](https://github.com/Rdatatable/data.table/issues/1377) grouping sets
- [ ] works on sharded distributed engine using [big.data.table](https://gitlab.com/jangorecki/big.data.table)

# Installation

```r
install.packages("data.cube", repos = paste0("https://", c(
    "jangorecki.gitlab.io/data.cube",
    "cloud.r-project.org"
)))
```

# Usage

Read [manual](https://jangorecki.gitlab.io/data.cube/library/data.cube/html/00Index.html) and check [*Subset and aggregate multidimensional data with data.cube*](https://jangorecki.gitlab.io/data.cube/library/data.cube/doc/sub-.data.cube.html) vignette.

# Contact

`j.gorecki@wit.edu.pl`
