# Digital Earth Pacific Water Observations from Space (WOfS)

This is a deployment of the Digital Earth Australia (DEA) WOfS algorithm for
Digital Earth Pacific. Primary changes to the DEA algorithm are utilization of
Landsat Collection 2 Level 2 and Copernicus Global 30-meter DEM for source data.

For a description of the WOfS algorithm, please see:

> Mueller, N., Lewis, A., Roberts, D., Ring, S., Melrose, R., Sixsmith, J.,
> Lymburner, L., McIntyre, A., Tan, P., Curnow, S., Ip, A., 2016. Water
> observations from space: Mapping surface water from 25 years of Landsat
> imagery across Australia. Remote Sensing of Environment 174, 341–352.
> https://doi.org/10.1016/j.rse.2015.11.003

This repository contains workflows to produce daily WOfS data (aka
WOFL) as well as annual and all-time summaries.

## Project Structure

### `dep_wofs/`

The `dep_wofs/` subfolder contains code to run the processing.

### `data/`

The data folder contains data necessary for processing

### `validation/`

This folder contains independent validation data and code

### `.argo/`

Processing at scale was accomplished using [Argo workflows](https://argoproj.github.io/).
This folder contains workflows used to produce all data outputs.

## Installation

This project requires a separate installation of GDAL (version 3.8.x;
3.8.4 was used for this project).

The code can be installed using pip, e.g.

```
pip install git+https://github.com/digitalearthpacific/dep-wofs.git
```

## Usage
