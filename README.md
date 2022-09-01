# Prism Weather Data

This repository provides a framework for directly downloading weather data from Oregon State's [Parameter-elevation Regressions on Independent Slopes Model (PRISM)](https://prism.nacse.org/) and works in conjunction with [rOpenSci.org/prism](https://docs.ropensci.org/prism/). 

Included is code that recovers PRISM dailys directly from the API, downloads to a temporary folder, reads a provided polygon shapefile (here, I use the [National Hydrography Dataset Plus](https://www.epa.gov/waterdata/nhdplus-national-data) data at the HUC-12 level), extracts the average weather variable of interest (precipitation used here), takes the average of that weather variable across the polygon, and exports a lightweight `.feather` file for use in analyses. 

Two scripts are provided.

- `get_prisim_loop.R`: This code will execute the extraction one day at a time across the specified timeframe. 
- `get_prism_parallel.R` This code will recover the available number of processors available on your machine, and run the extraction in parallel. 
