# array can be subsetted by dimension names but also by integer position
# in cube direct integer location is not possible but you can have surrogate key as integers!

library(data.table)
library(data.cube)
set.seed(1)

# tiny example -----------------------------------------------------------

# array
ar = array(rnorm(8,10,5), rep(2,3), dimnames = list(color = c("green","red"), year = c("2014","2015"), country = c("UK","IN")))
ar["green","2015",]
ar["green",c("2014","2015"),]
ar[,"2015",c("UK","IN")]

# cube normalized to star schema
cb = as.cube(ar)
cb["green","2015",]
cb["green",c("2014","2015"),]
cb[,"2015",]
cb[,"2015",c("UK","IN")]

# subset cube -------------------------------------------------------------

# as.cube.list - investigate X to see structure
X = populate_star(N=1e6, surrogate.keys = FALSE)
cb = as.cube(X)
cb
cb$keys

# slice
cb["Mazda RX4"]
cb[,,"BTC"]
cb["Mazda RX4",,"BTC"]
# dice
cb[,, c("CNY","BTC"), c("GA","IA","AD")]

# use dimensions hierarchy attributes for slice and dice, mix filters from various levels in hierarchy
cb["Mazda RX4",,.(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
cb[,,.(curr_type = "crypto"),.(geog_abb = c("AL","AK","CA"), geog_division_name = "Pacific")]

# lookup new columns with not filter on it by using NULL
cb[.(prod_cyl = 4L, prod_vs = 1L, prod_am = NULL),,, .(geog_abb = c("AL","TX","NV"), geog_division_name = NULL, geog_region_name = NULL)]

# lookup whole dimension
cb[NULL,, .(curr_type = "crypto")] # TO DO

# cube `[` operator returns data.table so it can be chainable
cb[,,, .(geog_abb = c("AL","TX","NV"), geog_division_name = NULL, geog_region_name = NULL)
   ][, .N, .(geog_region_name, geog_division_name)]

# estimated size of memory required to store an base R `array` for single measure
sprintf("array: %.2f GB", (prod(cb$nr[cb$dims]) * 8)/(1024^3))
# `cube` object with multiple measures, 
sprintf("cube: %.2f GB", sum(cb$mb)/1024)
