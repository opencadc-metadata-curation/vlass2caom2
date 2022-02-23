# vlass2caom2
Application to generate a CAOM2 observation from VLASS FITS files

# Accessing VLASS data from CADC

The VLASS metadata is available on a production server. All VLASS metadata and data at CADC is public, therefore there is no need to log in to access the CADC query or data retrieval services.

Documentation:
  - Browser UI: http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/en/doc/advancedsearch/
  - TAP: http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/en/doc/tap/
  - CLI for direct file download: http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/en/doc/data/

CLI for accessing files directly:
  - Common command line tools such as curl and wget can be used to retrieve VLASS files.
  - Cutout operations as described on the documentation page are also supported for VLASS data.
  - A VLASS example: curl -O -L http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/pub/VLASS/VLASS1.1.ql.T03t30.J194539-313000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits

Production services:
  - The browser UI is available here: http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/en/search/.
  - The URL of the TAP (Table access protocol) service is: http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus.
  - An example VLASS TAP synchronous query (note adding sync to the URL): curl http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus/sync -n -L -d "LANG=ADQL&FORMAT=TSV" --data-urlencode "QUERY=select count(*) as numObservations from caom2.Observation where collection='VLASS'"
  - One could also use the Topcat tool (http://www.star.bris.ac.uk/~mbt/topcat/) to query.


# Development Notes

- data available from https://archive-new.nrao.edu/vlass/quicklook/
- metadata available from https://archive-new.nrao.edu/vlass/weblog/quicklook/


# How To Run VLASS

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. In the master branch of this repository, find the scripts directory, and copy the file vlass_run_state.sh to the working directory. e.g.:

  ```
  wget https://raw.github.com/opencadc/vlass2caom2/master/scripts/vlass_run_state.sh
  ```

2. Ensure the script is executable:

```
chmod +x vlass_run_state.sh
```

3. Copy a valid proxy certificate to the working directory (cadcproxy.pem). This certificate must be for a user with read and write permissions to /ams service for CIRADA/VLASS.

4. To run the application:

```
./vlass_run_state.sh
```

# How To Run VLASS Reconciliation

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. In the master branch of this repository, find the scripts directory, and copy the file reconcile_list_vlass.sh to the working directory. e.g.:

  ```
  wget https://raw.github.com/opencadc/vlass2caom2/master/scripts/reconcile_list_vlass.sh
  ```

2. Ensure the script is executable:

```
chmod +x reconcile_list_vlass.sh
```

3. To run the application:

```
./reconcile_list_vlass.sh
```

The reconciliation output will be printed to the screen. Details will be captured in the file validate_state.yml.

