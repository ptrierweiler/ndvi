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
            gtif_list.append(homedir +'/'+i)

    # Ingetsting images into postgres
    for tif in gtif_list:
        schema = os.path.basename(tif).split('.')[0].lower()
        layer = '.'.join(os.path.basename(tif).split('.')[1:3]).lower().replace('.','_')
        os.system("raster2pgsql -C -I {tif} -d {schema}.{layer} \
        | psql patrick".format(tif=tif, schema=schema, layer=layer))

    # Seaching for rasters in the db
    srch_layer = '_'.join(layer.split('_')[0:2])
    conn = psycopg2.connect("dbname=patrick")
    cur = conn.cursor()
    cur.execute("select r_table_name from raster_columns where \
    r_table_schema = '{schema}' and r_table_name ~ '{srch_layer}'".format(
    schema = schema, srch_layer = srch_layer))
    db_list = cur.fetchall()
    # assigning rasters to variables
    for i in db_list:
        if i[0].split('_')[-1] == 'b01':
            rast1 = i
        elif i[0].split('_')[-1] == 'b02':
            rast2 = i
    # creating ndvi output
    ndvi = rast1.replace('b01','ndvi')
    # create ndvi table
    cur.execute("create table {schema}.{ndvi} \
    (rid int primary key, rast raster)".format(schema = schema, ndvi = ndvi))

    cur.execute("insert into mod09q1.ndvi(rid,rast) select \
     1,st_mapalgebra(r1.rast,1,r2.rast,1, 'case when [rast1.val] + [rast2.val] = 0 then Null else ([rast1.val] - [rast2.val])/([rast1.val] + [rast2.val]) end'::text,'32BF'::text,Null) from mod09q1.a2016033_h12v10_sur_refl_b01 as r1, mod09q1.a2016033_h12v10_sur_refl_b02 as r2")
