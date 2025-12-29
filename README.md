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

### [dep_wofs/](dep_wofs/)

The subfolder contains code to process individual tiles.

The most relevant files are:

- [process_wofl_item.py](dep_wofs/process_wofl_item.py)
  Use to create WOFL for a single Landsat STAC item.

- [process_wofls_annual.py](dep_wofs/process_wofls_annual.py)
  Use to create WOFLs for all Landsat scenes in a particular pathrow
  for an entire year.

- [process_wofls_recent.py](dep_wofs/process_wofls_recent.py)
  Use to create WOFLs for recent Landsat scenes in a particular pathrow.

- [process_wofs_full_history_tile.py](dep_wofs/process_wofs_full_history_tile.py)
  Use to create WOFS summaries across all WOFL scenes in a particular pathrow
  across all years.

- [process_wofs_tile.py](dep_wofs/process_wofs_tile.py)
  Use to create annual WOFS summaries for a particular pathrow.

### [data/](data/)

The data folder contains data necessary for processing.

### [validation/](validation/)

This folder contains independent validation data and code.

### [.argo/](.argo/)

Processing at scale was accomplished using [Argo workflows](https://argoproj.github.io/).
This folder contains workflows used to produce all data outputs.

- [wofls.yaml](.argo/wofls.yaml)
  For creating WOFL (scene-level) data.

- [wofs.yaml](.argo/wofs.yaml)
  For creating WOFS annual summaries.

- [wofs_full_history.yml](.argo/wofs_full_history.yml)
  For creating all-time WOFS summaries.

- [wofl-fc-cron-yaml](.argo/wofl-fc-cron.yaml)
  Cron-based workflow to process and index recent Landsat scenes, creating
  WOFL _and_ fractional cover.

## Installation

This project requires a separate installation of GDAL (version 3.8.x;
3.8.4 was used for this project).

The code can be installed using pip, e.g.

```
pip install git+https://github.com/digitalearthpacific/dep-wofs.git
```

## Usage

You could create WOFL data for a single Landsat scene doing something like this

```python
import odc.stac
import pystac
import rioxarray
from dep_wofs.processors import wofl

odc.stac.configure_s3_access(requester_pays=True, cloud_defaults=True)

item = pystac.Item.from_file("https://earth-search.aws.element84.com/v1/collections/landsat-c2-l2/items/LC09_L2SR_081072_20251208_02_T1")
landsat_ds = odc.stac.load([item])
wofl_for_scene = wofl(landsat_ds)
```

WOFS summaries could be created for data produced using `wofl` at multiple times:

```python
from dep_wofs.processors import wofs

# ... first produce WOFL for multiple times in the same place

wofs = wofs(wofls_for_scenes)
```
