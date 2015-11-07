# 
# # i preprocess
# ## dimensions
# ### for `[[` catch by names of `i` list(...)
# ### for `[` catch by position in ... - `[` stores all dimensions anyway!
# ## filters
# ### for `[[` supports (allow to mix)
# #### by list of values in each filter: i = .(currency = .(curr_name = c("a","b"), curr_type = "z"), time = ...)
# #### by expression to evaluate in dimension: i = .(currency = curr %in% c("a","b"), time = ...)
# ### for `[` supports (allow to mix)
# #### by atomic scalar/vectors: x["green", c("2012","2012")]
# #### by same as `[[`
# # apply filters on required dimensions
# # inner join required dimensions to fact
# # evaluate `j` by groups of `by`
# 
# dimnames = list(color = sort(c("green","yellow","red")), 
#                 year = as.character(2011:2015), 
#                 status = sort(c("active","inactive","archived","removed")))
# ar = array(sample(c(rep(NA, 4), 4:7/2), prod(sapply(dimnames, length)), TRUE), 
#            unname(sapply(dimnames, length)), 
#            dimnames)
# as.cube(ar)["green",,"active",drop=FALSE]
# as.cube(ar)["green",,c("active","inactive"),drop=FALSE]
# as.cube(ar)["green",,NULL,drop=FALSE]
# 
# dla()
# cb = as.cube(populate_star(1e4))
# cb["Mazda RX4",, .(curr_type = "crypto")]
# cb["Mazda RX4",, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# x = "Mazda RX4"
# cb[x,, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# cb[NULL,, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# cb[character(),, .(curr_type = "crypto"),, .(time_year = 2014L, time_quarter_name = c("Q1","Q2"))]
# 
# # dev ----
# 
# dla = devtools::load_all
# dla()
# set.seed(1L)
# X = populate_star(1e5s)
# cb = as.cube(X)
# 
# cb[[.(currency = .(curr_name = c("BTC","EUR"), curr_type = "crypto"), time = .()), .(value=sum(value)), .(time_year, curr_type)]]
# 
# cb[[.(currency = .(), time = .()), .(value=sum(value)), .(time_year, curr_type)]]
# 
# cb[[, .(value=sum(value)), .(time_year, curr_type)]]
# 
# cb[[.(), .(value=sum(value)), .(time_year, curr_type)]]
