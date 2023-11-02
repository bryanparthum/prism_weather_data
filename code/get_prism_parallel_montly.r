###############################################################################
###########################    Get PRISM Weather    ###########################
###############################################################################

## install packages if they are not on the machine
## examples and info available here: https://docs.ropensci.org/prism/
# install.packages("devtools")
# devtools::install_github(repo="prism",username="ropensci")
# devtools::install_github("UrbanInstitute/urbnmapr")

##########################
#################  library
##########################

## clear worksace
rm(list = ls())
gc()

## This function will check if a package is installed, and if not, install it
list.of.packages <- c('magrittr','tidyverse','dplyr','data.table','lubridate',
                      'arrow',
                      "prism",
                      'sf', 'USAboundaries',
                      'raster', 'exactextractr',
                      'foreach', 'doParallel',
                      'archive')
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages, repos = "http://cran.rstudio.com/")
lapply(list.of.packages, library, character.only = TRUE)

##########################
###################  parts
##########################

## get boundary polygon of interest
boundary =
  us_states(states = c('Ohio')) %>%
  st_make_valid

## get shape from nhd
options(timeout = 200)
tf           = tempfile()
destination  = paste0(getwd(),'/../data/')
url          = 'https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NationalData/NHDPlusV21_NationalData_WBDSnapshot_Shapefile_08.7z'
download.file(url , tf)
unzip(tf)

## import raw nhd data and trim to desired boundary shape
sf::sf_use_s2(FALSE) ## this is an issue with a particular version of sf. fix later
polys =
  st_read('../data/NHDPlusV21_NationalData_WBDSnapshot_Shapefile_08/NHDPlusNationalData/WBDSnapshot_National.shp') %>%
  rename_all(., .funs = tolower) %>%
  st_intersection(st_transform(boundary, st_crs(.)))

## export to skip computation time above
if (!dir.exists('../data/huc12_polygons')) {dir.create('../data/huc12_polygons', recursive = T)}
polys %>% st_write('../data/huc12_polygons/huc12_polygons.shp')

## open a prism raster and reproject so that the huc12s are already in the correct projection
if (!dir.exists('../data/prism_temporary')) {dir.create('../data/prism_temporary/', recursive = T)}
options(prism.path = '../data/prism_temporary')
get_prism_dailys(type = "ppt", minDate = "1999-12-31", maxDate = "1999-12-31", keepZip = F)
dir    = '../data/prism_temporary'
subdir = list.files(dir)
path   = list.files(dir, full.names = T)
raster = raster(paste(path, "/", subdir, ".bil", sep = ""))
polys =
  st_read('../data/huc12_polygons/huc12_polygons.shp') %>%
  st_transform(st_crs(raster))
unlink(dir, recursive = T)

## export to skip computation time above
if (!dir.exists('../data/huc12_polygons_with_prism_crs')) {dir.create('../data/huc12_polygons_with_prism_crs', recursive = T)}
polys %>% st_write('../data/huc12_polygons_with_prism_crs/huc12_polygons_with_prism_crs.shp')

## check projections
plot(polys$geometry)
plot(raster, add = T)

## read from above
polys = 
  st_read('../data/huc12_polygons_with_prism_crs/huc12_polygons_with_prism_crs.shp') %>% 
  dplyr::select(huc_12, geometry)

##########################
##########  set up cluster
##########################

## parallel filter and writing of feather files
parallel::detectCores()
# n.cores <- parallel::detectCores() - 1
n.cores = 12

## make cluster
my.cluster <- parallel::makeCluster(
  n.cores, 
  type = "PSOCK"
)

## register it to be used by %dopar%
doParallel::registerDoParallel(cl = my.cluster)

##########################
#################  execute
##########################

# for (VAR in c('ppt', 'tmin', 'tmax')) {
for (VAR in c('ppt')) {
  
  cat('\nreading data for', VAR, '\n')
  
  tryCatch( ## in case error is triggered
  
      foreach(DATE      = 2000:2023, 
            .packages = c('tidyverse',
                          'lubridate',
                          'arrow',
                          "prism",
                          'sf',
                          'raster', 
                          'exactextractr')) %dopar% {
                            
                            ## create temporary directory if it doesn't yet exist
                            if (!dir.exists(paste0('../data/prism_temporary/', VAR, '/', DATE))) {dir.create(paste0('../data/prism_temporary/', VAR, '/', DATE), recursive = T)}
                            dir = paste0('../data/prism_temporary/', VAR, '/', DATE)
                            
                            ## set destination for PRISM raw .bil files
                            options(prism.path = dir) 
                            get_prism_monthlys(type = VAR, year = DATE, mon = 1:12, keepZip = F)
                            
                            ## paths
                            subdir = list.files(dir)
                            path   = list.files(dir, full.names = T)
                            files  = paste(path, "/", subdir, ".bil", sep = "")
                            
                            ## create directory if it doesn't yet exist
                            if (!dir.exists(paste0('../data/prism_monthly/', VAR, '/', DATE))) {dir.create(paste0('../data/prism_monthly/', VAR, '/', DATE), recursive = T)}
                            
                            ## extract
                            for (i in 1:12){
                              ## get month
                              moy = str_split(basename(files[i]), pattern = '_')[[1]][5]
                              date = paste0(substr(moy, start = 1, stop = 4), '-', substr(moy, start = 5, stop = 7))
                              
                              ## do extraction and write file
                              polys %>% 
                                mutate(!!sym(VAR) := exact_extract(raster(files[i]), polys, 'mean')) %>% 
                                st_drop_geometry %>% 
                                write_feather(paste0('../data/prism_monthly/', VAR, '/', DATE, '/', VAR, '-', date, '.feather'))
                            }
                            
                            ## delete prism file directory
                            unlink(dir, recursive = T)
                            
                            ## clean house
                            gc()
                          },
      error = function(e) cat('could not read data for ', VAR))
}

## stop cluster
parallel::stopCluster(cl = my.cluster)

## end of script, have a great day!