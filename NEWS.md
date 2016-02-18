# data.cube 0.2.1

* `levels` kept in `dimension` instead `hierarchy`.
* `fact$query` works for local and remote, `i` / `i.dt`.
* `schema` methods to produce denormalized metadata.
* added `logR` to *Suggests*, added to tests, heavy non-R dep for postgres drivers.
* added `time_week` and `time_weekday` attributes of `time_day` level in time dimension.
* CRAN check cleanup.
* `big.data.table` + `data.cube` vignette draft.
* new classes to handle low lewel metadata: *level, hierarchy, dimension, measure, fact, data.cube*.
* `fact` and `data.cube` classes can be used against distributed data.table via [big.data.table](https://gitlab.com/jangorecki/big.data.table).
* update CI to artifacts.
* `cube` subset vignette.

# data.table 0.2

* working `cube` class.
