add.surrogate.key = function(x){
    nm = copy(names(x))
    id_col = paste(strsplit(nm[1L], "_", fixed = TRUE)[[1L]][1L], "id", sep="_")
    qj = as.call(lapply(c(":=", id_col, ".I"), as.symbol))
    x[, eval(qj), c(nm[1L])]
    setcolorder(x, c(id_col, nm))
}

as.data.table.array = function(x) do.call(CJ, dimnames(x))[, .(value = eval(as.call(lapply(c("[","x", names(dimnames(x))), as.symbol)))),, keyby = c(names(dimnames(x)))]

#' @title Populate star schema tables
#' @param N integer count of rows in fact table before sub-aggregation to all dimensions.
#' @param surrogate.keys logical
#' @param seed integer used for `set.seed` when producing fact table from dimensions.
#' @description Populates example sales data based on *mtcars*, *state*, *HairEyeColor* datasets.
#' @return List of two list named *fact* and *dims*. The *fact* list keeps single fact data.table sub-aggregated to all dimensions. The *dims* list keeps five dimension data.tables.
populate_star = function(N, surrogate.keys = FALSE, seed = 1L){
    # produce dimension based on mtcars dataset
    product = as.data.table(mtcars, keep.rownames = "prod_name")[, .(prod_name, prod_cyl = as.integer(cyl), prod_vs = as.integer(vs), prod_am = as.integer(am), prod_gear = as.integer(gear))]
    # customer dimension based on HairEyeColor dataset
    customer = as.data.table.array(HairEyeColor)[, .(cust_profile = as.character(.I), cust_hair = Hair, cust_eye = Eye, cust_sex = Sex)]
    # geography dimension based on state.* dataset
    geography = data.table(geog_abb = state.abb, geog_name = state.name, geog_division_name = as.character(state.division), geog_region_name = as.character(state.region))
    # time dimension based on sequence of Dates
    time = data.table(time_date = seq(as.Date("2010-01-01"), as.Date("2014-12-31"), by = "1 day"))
    time[,`:=`(time_month = month(time_date),
               time_quarter = as.POSIXlt(time_date)$mon %/% 3L + 1L,
               time_year = year(time_date))
         ][,`:=`(time_month_name = month.name[time_month],
                 time_quarter_name = paste0("Q",time_quarter))
           ]
    setcolorder(time, c("time_date", "time_month", "time_month_name", "time_quarter", "time_quarter_name", "time_year"))
    # currency taken from: https://github.com/jangorecki/Rbitcoin/blob/cf142d34e8018d135a526998aeddc4e28d548f5c/R/dictionaries.R#L139
    ct.dict = list(
        crypto = c('BTC','LTC','NMC','FTC','NVC','PPC','TRC','XPM','XDG','XRP','XVN'),
        fiat = c('USD','EUR','GBP','KRW','PLN','RUR','JPY','CHF','CAD','AUD','NZD','CNY','INR',
                 'TRY','SYP','GEL','AZN','IRR','KZT','NOK','SEK','ISK','MYR','DKK','BGN','HRK',
                 'CZK','HUF','LTL','RON','UAH','IDR','IQD','MNT','BRL','ARS','VEF','MXN')
    )
    currency = rbindlist(lapply(1:length(ct.dict), function(i) data.table(curr_name = ct.dict[[i]], curr_type = names(ct.dict[i]))))
    # wrap dimensions into list
    dims = list(product=product, customer=customer, currency=currency, geography=geography, time=time)
    # add surrogate keys if needed
    if(surrogate.keys){
        lapply(dims, add.surrogate.key)
    }
    # add key on first col
    lapply(dims, function(x) setkeyv(x, names(x)[1L]))
    # build fact table
    keys = sapply(dims, key)
    set.seed(seed)
    sales = setDT(lapply(setNames(names(keys), keys), function(dim) sample(dims[[dim]][[1L]], N, TRUE)))
    sales[, `:=`(amount = round(runif(N, max=1e2), 2),
                 value = round(runif(N,max=1e4),4))]
    sales = sales[, lapply(.SD, sum),, c(keys)]
    # wrap into lists
    list(fact = list(sales=sales), dims = dims)
}
