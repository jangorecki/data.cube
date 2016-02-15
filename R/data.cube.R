## http://stackoverflow.com/a/16054159/2490497
#<Schema>
# <Cube>
#  <Dimension Gender>
#    <Hierarchy>
#      <Level Gender>
#    </Hierarchy>
#  </Dimension>
#  <Dimension Time>
#    <Hierarchy>
#      <Level Year/>
#      <Level Quarter/>
#      <Level Month/>
#    </Hierarchy>
#    <Hierarchy>
#      <Level Year/>
#      <Level Week/>
#      <Level Day/>
#    </Hierarchy>
#  </Dimension>
#  <Measure Unit Sales/>
#  <Measure Store Sales/>
# </Cube>
#</Schema>

level = R6Class(
    classname = "level",
    public = list(
        key = character(),
        properties = character(),
        # level-data stores hierarchy level key and it's related attributes
        data = NULL,
        initialize = function(x, key = key(x), properties){
            stopifnot(is.data.table(x), is.character(key), key %in% names(x), is.character(properties), properties %in% names(x))
            self$key = key
            rm(key)
            self$properties = unique(properties)
            self$data = setkeyv(unique(x, by = self$key)[, j = .SD, .SDcols = unique(c(self$key, self$properties))], self$key)[]
            invisible(self)
        }
    )
)

hierarchy = R6Class(
    classname = "hierarchy",
    public = list(
        key = character(),
        # just list of levels
        levels = list(),
        initialize = function(x, key = key(x), levels){
            stopifnot(is.data.table(x), is.character(key), key %in% names(x), is.list(levels), as.logical(length(levels)), names(levels) %in% names(x))
            self$key = key
            rm(key)
            self$levels = lapply(setNames(nm = names(levels)), function(nm){
                level$new(x, key = nm, properties = unique(levels[[nm]]))
            })
            invisible(self)
        }
    )
)

dimension = R6Class(
    classname = "dimension",
    public = list(
        key = character(),
        hierarchies = list(),
        # dimension-data stores mapping from dimension key to of any of the level keys
        data = NULL,
        initialize = function(x, key = key(x), hierarchies){
            stopifnot(is.data.table(x), is.character(key), key %in% names(x), is.list(hierarchies), as.logical(length(hierarchies)))
            stopifnot(
                # level keys in data.table
                (all.hierarchies.level.keys <- unique(unlist(lapply(hierarchies, names)))) %in% names(x),
                # level attributes in data.table
                (all.hierarchies.level.attrs <- unique(unname(unlist(hierarchies, recursive = TRUE)))) %in% names(x)
            )
            self$key = key
            rm(key)
            self$hierarchies = lapply(hierarchies, function(lvls) hierarchy$new(x, key = self$key, levels = lvls))
            # all.hierarchies.level.mappings
            granularity = unique(c(self$key, all.hierarchies.level.keys))
            self$data = setkeyv(unique(x, by = granularity)[, .SD, .SDcols = granularity], self$key)[]
            invisible(self)
        }
    )
)


# fact = R6Class(
#     classname = "fact",
#     public = list(
#         local = logical(),
#         data = NULL,
#         initialize = function(x){
#             
#         }
#     )
# )

# cube = R6Class(
#     classname = "cube",
#     public = list(
#         data = NULL,
#         dimensions = list(),
#         initialize = function(fact, dimensions){
#             stopifnot(is.data.table(x), sapply(dimensions, inherits, "dimension"), inherits(fact))
#             
#             invisible(self)
#         }
#     )
# )
# 
# as.cube.default = function(x, ...){
#     dimensions = lapply(dimensions, function(hierarchies) dimension$new(x, hierarchies = hierarchies))
#     
#     #self$fact = setkeyv(x[, .SD, .SDcols = unique(granularity)], granularity)[]
#     
#     cube$new(fact = fact$new(x, granularity = unique(unlist(lapply(dimensions, `[[`, "key"))) ),
#              dimensions = lapply(dimensions, function(hierarchies) dimension$new(x, key = "", hierarchies = hierarchies))
#              )
# }

# data.cube = R6Class(
#     classname = "data.cube",
#     public = list(
#         local = logical(),
#         initialize = function(x){
#             
#         }
#     )
# )

# as.data.cube.function = function(){
#     # remote
#     
# }
# 
# as.data.cube.data.table = function(){
#     # local
#     
# }

