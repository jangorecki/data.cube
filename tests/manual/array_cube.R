library(data.table)

set.seed(1)
prod = paste0("prod ",1:1e3,sample(letters,1e3,TRUE))
geog = state.abb
time = c(sprintf("2014-%02.f", 1:12), sprintf("2015-%02.f", 1:6))
curr = c("CNY","BTC","GBP","USD","EUR")

N = 1e6
sales <- data.table(
    prod = sample(prod, N, TRUE),
    geog = sample(geog, N, TRUE),
    time = sample(time, N, TRUE),
    curr = sample(curr, N, TRUE),
    amount = round(runif(N,max=1e2),4)
)
sales = sales[,.(amount=sum(amount)),.(prod,geog,time,curr)]

array_cube = tapply(sales$amount, sales[,c("prod", "geog", "time", "curr"),  with=FALSE], FUN=function(x){return(sum(x, na.rm=TRUE))})

# Slice
array_cube["prod 474k", , "2015-01",]
array_cube["prod 9q", "IL", "2015-01",]

# Dice
array_cube[c("prod 9q","prod 807j"), c("IL","IA"), c("2015-01","2014-07"),]

# Rollup
r = apply(array_cube, c("prod","time"), FUN=function(x) {return(sum(x, na.rm=TRUE))})
str(r)
r[1:3,] # only for 3 products

# Drilldown
r = apply(array_cube, c("geog", "time", "prod"), FUN=function(x) {return(sum(x, na.rm=TRUE))})
str(r)
r[1:3, 1:3, 1:3] # 3 prod, 3 time, 3 geog

# Pivot
r = apply(array_cube, c("geog", "prod"), FUN=function(x) {return(sum(x, na.rm=TRUE))})
str(r)
r[1:10, 1:3] # 10 geog, 3 prods

## array cubes issues
# grows exponentially with number/cardinality of dimension
# store aggregates all dimension combinations, potentially a lot of NULLs (NA)
# no hierarchy, query only using lowest granularity keys
# single measure per cube, can be tricked by measure type dimension
# play with volume of data and cardinality of dimension to check scalability

prod(dim(array_cube))
length(array_cube)

format(object.size(array_cube), units="MB")
format(object.size(sales), units="MB")

sum(!is.na(array_cube)) # actual measure values
sum(is.na(array_cube)) # redundant array values

# what if we use time date granularity instead of year-month?
len = prod(c(dim(array_cube)[-3], 18*30)) # cardinality: time * 30
format(object.size(numeric(len)), units="MB")

# what if we also have 10 currencies instead of 5?
len = prod(c(dim(array_cube)[1:2], 18*30, 10)) # cardinality: time * 30, currency * 2
format(object.size(numeric(len)), units="MB")
