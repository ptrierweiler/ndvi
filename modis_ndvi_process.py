#!/usr/bin/python2
# Downloads modis tile ingest and calculates ndvi
import psycopg2
import sys
import os
import argparse
import datetime
import pymodis

sat='MOLT'
prod = 'MOD09Q1.005'
tile = 'h13v11'
year = 2016
st_doy = 1
ed_doy = 9

homedir = os.path.expanduser('~') +'/modis'

st_pydate = datetime.datetime.strptime(str(year)+str(st_doy).zfill(3),'%Y%j')

st_date = st_pydate.strftime('%Y-%m-%d')

doy_tuples = tuple(range(st_doy,ed_doy,8))

for d in doy_tuples:
    doy = str(d).zfill(3)
    ed_pydate = datetime.datetime.strptime(str(year)+doy,'%Y%j')
    ed_date = ed_pydate.strftime('%Y-%m-%d')

    #Creating modis download object
    pydown = pymodis.downmodis.downModis(homedir,
    password=None, user='anonymous', url='http://e4ftl01.cr.usgs.gov',
    tiles=tile, path=sat, product=prod,
    today=st_date,enddate=ed_date)

    #Connecting and downloading
    pydown.connect()
    print "Downloading image"
    pydown.downloadsAllDay()

    # Obtaining list of hdf files
    file_list = []
    for i in os.listdir(homedir):
        if i.split('.')[-1] == 'hdf':
            file_list.append(i)
    os.
    for f in file_list:
        hdf = homedir + '/' + f
        hdf_pre = '.'.join(hdf.split('.')[0:3])
        con = pymodis.convertmodis_gdal.convertModisGDAL(hdfname=hdf, outformat='GTiff', epsg=29101, subset=[1,1,1], res=250, prefix=hdf_pre)
        # unpacking HDF, reprojecting and converting to Gtiff
        con.run()
        print "Deleting HDF file: " + f
        os.remove(hdf)

    #Obtaining list of gtiff files
    gtif_list = []
    for i in os.listdir(homedir):
        if i.split('.')[-1] == 'tif':
            gtif_list.append(i)

    #Ingetsting images into postgres
    
