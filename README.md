transitpy
==============================
[//]: # (Badges)
[![GitHub Actions Build Status](https://github.com/chourmo/transitpy/workflows/CI/badge.svg)](https://github.com/chourmo/transitpy/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/chourmo/transitpy/branch/master/graph/badge.svg)](https://codecov.io/gh/chourmo/transitpy/branch/master)


**Transitpy** is a python library to parse, normalize and extract information a GTFS file. It focuses on statistics and analysis.

### Description

**Transipty** encapsulates a GTFS directory of files into an object. A basic datasource function is provided for the french GTFS repository (https://transport.data.gouv.fr).

In order to provide consistent statistics, GTFS files are normalised :
    - fill default values, departure and arrival_times and create a default shape data
    - remove incoherent ids
    - simplify stops representations by keeping stations only
    - identify and remove stops with impossible coordinates based on a maximum speed by mode
    - expand and remove the calendar data into a unique calendar_date, without invalid dates
    - remove invalid data (e.g. stop_times with one stop)
    - create simple geometries and add a crs
    - create a group_id for routes with similar stop patterns

Non conforming route or stop ids are stored in a dropped property of the feed object.

**Transitpy** also provides modal, spatial and temporal filtering functions.

**Transitpy** has functions to extract a transfer dataframe with itself or another GTFS feed (bus and rail network for example).

**Transitpy** extracts stop, route and transfer statistics.

**Transitpy** can update a shape file to map on a streetpy (https://githbu.com/chourmo/streetpy) based on a modified HMM algorithm.


**Documentation** is available at [https://transitpy.readthedocs.io](https://streetpy.readthedocs.io/en/latest/).

### Copyright

Copyright (c) 2022, chourmo


#### Acknowledgements
 
Project based on the 
[Computational Molecular Science Python Cookiecutter](https://github.com/molssi/cookiecutter-cms) version 1.6.
