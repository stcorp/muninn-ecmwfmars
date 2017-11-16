muninn-ecmwfmars
================

This python package facilitates the deployment of a muninn archive of ECMWF,
providing:
- a muninn namespace extension
- a muninn remote backend to retrieve data from MARS (requires the
  ecmwf-api-client library and MARS credentials)
- helper functions:

  - extract_grib_metadata: retrieve ECMWF from a file (requires coda and
    numpy)
  - get_remote_url: build a MARS URL
  - get_core_properties: get muninn core properties from ECMWF metadata
