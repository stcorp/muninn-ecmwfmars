muninn-ecmwfmars
================

This python package facilitates the deployment of a muninn archive containing
ECMWF grib data, providing:

- a muninn namespace extension
- a muninn remote backend to retrieve data from MARS
  (requires the ecmwf-api-client library and MARS credentials)
- helper functions:

  - extract_grib_metadata: retrieve ECMWF metadata from a file
    (requires coda and numpy)
  - get_remote_url: build a MARS URL for the muninn remote backend
  - get_core_properties: get muninn core properties from ECMWF metadata


This package requires muninn 5.0 or higher.
