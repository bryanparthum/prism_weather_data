## written by US EPA National Center for Environmental Economics, August 2022

###############################################################################
###########################    Get PRISM Weather    ###########################
###############################################################################

# ## Install packages if they are not on the machine
# ## examples and info available here: https://docs.ropensci.org/prism/
# install.packages("devtools")
# devtools::install_github(repo="prism",username="ropensci")
# devtools::install_github("UrbanInstitute/urbnmapr")

##########################
#################  library
##########################

## Clear worksace
rm(list = ls())
gc()

## This function will check if a package is installed, and if not, install it
list.of.packages <- c('magrittr','tidyverse','dplyr','data.table','lubridate',
                      'arrow',
                      "prism",
                      "spData",'sf','tigris', 'urbnmapr',
                      'raster', 'exactextractr',
                      'tictoc',
                      'foreach', 'doParallel',
                      'archive')
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, repos = "http://cran.rstudio.com/")
lapply(list.of.packages, library, character.only = TRUE)

##########################
###################  parts
##########################

# # # ## get shape from nhd
# # # tf  = tempfile()
# # # url = 'https://edap-ow-data-commons.s3.amazonaws.com/NHDPlusV21/Data/NationalData/NHDPlusV21_NationalData_WBDSnapshot_Shapefile_08.7z'
# # # download.file(url , tf)
# # # archive_extract(tf)
# # 
# # ## import raw nhd data
# # polys =
# #   st_read('NHDPlusNationalData/WBDSnapshot_National.shp') %>%
# #   rename_all(., .funs = tolower)
# # 
# # # get usa map (conus only)
# # usa =
# #   get_urbn_map("states", sf = TRUE) %>%
# #   filter(!(state_abbv %in% c('AK','HI'))) %>%
# #   st_union
# # 
# # ## trim huc12 to conus
# # sf::sf_use_s2(FALSE) ## this is an issue with a particular version of sf. fix later
# # ## this seems to filter out 2,000 too many, maybe st_within is "entirely within"
# # # polys %<>%
# # #   dplyr::select(huc_12, geometry) %>%
# # #   st_filter(usa, .predicate = st_within)
# # tic()
# # polys = st_transform(polys, st_crs(usa))[usa,]
# # toc()
# # 
# # ## export to skip computation time above
# # polys %>% st_write('store/huc12_conus/huc12_conus.shp')
# 
# ## open a prism raster and reproject so that the huc12s are already in the correct projection
# options(prism.path = "store/temporary")
# get_prism_dailys(type = "ppt", minDate = "1999-12-31", maxDate = "1999-12-31", keepZip = F)
# dir    = "store/temporary"
# subdir = list.files("store/temporary")
# path   = list.files("store/temporary", full.names = T)
# raster = raster(paste(path, "/", subdir, ".bil", sep = ""))
# polys =
#   st_read('store/huc12_conus/huc12_conus.shp') %>%
#   st_transform(st_crs(raster))
# # ## check projections
# polys %>% st_write('store/huc12_conus_with_prism_crs/huc12_conus_with_prism_crs.shp')
# plot(polys)
# plot(raster, add = T)

## read from above
polys = 
  st_read('store/huc12_conus_with_prism_crs/huc12_conus_with_prism_crs.shp') %>% 
  dplyr::select(huc_12, geometry)

##########################
##########  set up cluster
##########################

## parallel filter and writing of feather files
# parallel::detectCores()
# n.cores <- parallel::detectCores() - 1
n.cores = 20

## make cluster
my.cluster <- parallel::makeCluster(
  n.cores, 
  type = "PSOCK"
)

## register it to be used by %dopar%
doParallel::registerDoParallel(cl = my.cluster)

## check if it is registered (optional)
foreach::getDoParRegistered()

## how many workers are available? (optional)
foreach::getDoParWorkers()

##########################
################  download
##########################

## export scenario-specific files to read into fredi
foreach(DATE      = as.character(seq(ymd("2000-01-01"), ymd("2022-07-31"), "days")), 
        .packages = c('tidyverse',
                      'lubridate',
                      'arrow',
                      "prism",
                      'sf',
                      'raster', 
                      'exactextractr')) %dopar% {
                        
                        # ## test date parameter
                        # DATE = "1999-12-02"
                        
                        ## create temporary directory if it doesn't yet exist
                        if (!dir.exists(paste0('store/prism_temporary/', DATE))) {dir.create(paste0('store/prism_temporary/', DATE), recursive = T)}
                        dir = paste0('store/prism_temporary/', DATE)
                        
                        ## set destination for PRISM raw .bil files
                        options(prism.path = dir) 
                        get_prism_dailys(type = "ppt", minDate = DATE, maxDate = DATE, keepZip = F) 
                        
                        ##########################
                        ################## extract
                        ##########################
                        
                        ## paths
                        subdir = list.files(dir)
                        path   = list.files(dir, full.names = T)
                        file   = paste(path, "/", subdir, ".bil", sep = "")
                        
                        ## create directory if it doesn't yet exist
                        if (!dir.exists(paste0('data/prism/precip/', year(DATE)))) {dir.create(paste0('data/prism/precip/', year(DATE)), recursive = T)}
                        
                        ## extract
                        polys %>% 
                          mutate(precip = exact_extract(raster(file), polys, 'mean')) %>% 
                          st_drop_geometry() %>% 
                          write_feather(paste0('data/prism/precip/', year(DATE), '/precip_', DATE, '.feather'))
                        
                        ## delete prism file directory
                        unlink(dir, recursive = T)
                        
                        ## clean house
                        gc()
                      }

## stop cluster
parallel::stopCluster(cl = my.cluster)

## End of script, have a nice day!