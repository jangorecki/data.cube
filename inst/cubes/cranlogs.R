library(data.table)
library(data.cube)

get_last_n_cranlogs = function(n = 3L, from = Sys.Date(), path = ".", dir = "cran-mirror"){
    ## based on arunsrinivasan/cran.stats
    # https://github.com/arunsrinivasan/cran.stats/blob/8e0fd7ea6c047e7d1645c54b662869353c65cdf0/R/cran.stats.R
    i = 0L
    odir = file.path(path, dir)
    src = character()
    dir.create(odir, showWarnings = FALSE, recursive = TRUE)
    # download last n
    while(length(src) < n){
        if(i > n + 10L) stop("R session could not reach cran-logs rstudio host. Alternatively cran-logs not available for last 10 days?")
        date = from - i
        url = paste0("http://cran-logs.rstudio.com/", year(date), "/", date, ".csv.gz")
        dest = paste0(as.character(date), ".csv.gz")
        r = tryCatch({
            if(!file.exists(file.path(odir, dest))){
                cat(sprintf("trying to download %s\n", url))
                suppressWarnings(download.file(url=url, destfile=file.path(odir, dest), quiet=TRUE, mode="wb"))
                cat(sprintf("file downloaded %s\n", dest))
            } else {
                cat(sprintf("skipping exisitng file %s\n", dest))
            }
            src[as.character(date)] = dest
            TRUE
        }, error = function(e){
            file.remove(file.path(odir, dest))
            NULL
        })
        i = i + 1L
    }
    # read gzipped
    rbindlist(lapply(src, function(file) read.table(file.path(odir, file), header = TRUE, sep = ",", quote = "\"", dec = ".", fill = TRUE, stringsAsFactors = FALSE, comment.char = "", as.is=TRUE)))
}

cranlogs_cube = function(x){
    x = copy(x)
    if(is.character(x$date)) x[, date := as.Date(date)]

    # make dims
    time_dim = x[, .(date = unique(date))
                 ][, .(date, month = month(date), year = year(date))
                   ][, .(time_id = .I, date, month, month_name = month.name[month], year, yearmonth = paste0(year, sprintf("%02d", month))),
                     ]
    r_dim = x[, .(r_id = .GRP), .(r_version, r_arch, r_os)
              ][, .(r_id, r_version, r_arch, r_os)]
    pkgv_dim = x[, .(pkgv_id = .GRP), .(package, version)
                 ][, .(pkgv_id, package, version)]

    # lookup surrogate keys
    x[r_dim, r_id := i.r_id, on = c("r_version", "r_arch", "r_os")]
    x[time_dim, time_id := i.time_id, on = c("date")]
    x[pkgv_dim, pkgv_id := i.pkgv_id, on = c("package","version")]

    # setkey on dims
    setkeyv(r_dim, "r_id")
    setkeyv(time_dim, "time_id")
    setkeyv(pkgv_dim, "pkgv_id")

    # wrap into cube
    cube$new(fact = list(cranlogs = x[, .(count = .N), .(time_id, r_id, pkgv_id)]),
             dims = list(time = time_dim, r = r_dim, pkgv = pkgv_dim))
}

dt = get_last_n_cranlogs()
cranlogs = cranlogs_cube(dt)
