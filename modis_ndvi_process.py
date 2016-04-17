#!/usr/bin/python2
# Downloads modis tile ingest and calculates ndvi
import psycopg2
import sys
import os
import argparse
import datetime
import pymodis

parser = argparse.ArgumentParser(description='Download and process Modis')

parser.add_argument('-s', type=str, metavar='MOLT', nargs='?', required=True,
                    help='The MODIS satilite Terra: MOLT or Agua: MOLA')
parser.add_argument('-p', metavar='MOD09Q1.006', type=str, nargs='?',
                     required=True, help='MODIS product')
parser.add_argument('-t', metavar='h13v11', type=str, nargs='?', required=True,
                    help='MODIS tile')
parser.add_argument('-y', metavar=2016, type=int, nargs='?',
                    default=datetime.datetime.now().year,
                    help='year as int defaults to current year')
parser.add_argument('-b', metavar=1, type=int, nargs='?',
                    default=1, help='Start doy as int, defauts 1')
parser.add_argument('-e', metavar=365, type=int, nargs='?',
                    default=365, help='End doy as int defaults to 365')

args = parser.parse_args()

sat = args.s
prod = args.p
tile = args.t
year = args.y
st_doy = args.b
ed_doy = args.e

print "sat: " + sat
print "prod: " + prod
print "tile: " + tile
print "year: " + str(year)
print "stdoy: " + str(st_doy)
print "eddoy: " + str(ed_doy)

def process_func(sat, prod, tile, year, st_doy, ed_doy):
    # Creating dates and paths
    schema = prod.split('.')[0].lower()
    homedir = os.path.expanduser('~') +'/modis'
    # Changing end doy for current year,
    # End doy should exceed the the current doy
    if year == datetime.datetime.now().year and ed_date == 365:
        ed_doy = datetime.datetime.now().strftime('%j').zfill(3)

    if ed_doy == 1:
        ed_doy = ed_doy + 1

    doy_tuples = tuple(range(st_doy,ed_doy,8))

    for d in doy_tuples:
        doy = str(d).zfill(3)
        doy2 = str(d + 1).zfill(3)
        doy_pydate = datetime.datetime.strptime(str(year)+str(doy).zfill(3),'%Y%j')
        doy2_pydate = datetime.datetime.strptime(str(year)+str(doy2).zfill(3),'%Y%j')
        doy_date = doy_pydate.strftime('%Y-%m-%d')
        doy2_date = doy2_pydate.strftime('%Y-%m-%d')
        print "Processing: " + str(doy)
        print doy_date

        #Creating modis download object
        pydown = pymodis.downmodis.downModis(homedir,
        password=None, user='anonymous', url='http://e4ftl01.cr.usgs.gov',
        tiles=tile, path=sat, product=prod,
        today=doy2_date,enddate=doy_date)

        #Connecting and downloading
        pydown.connect()
        print "Downloading image"
        pydown.downloadsAllDay()

        # Obtaining list of hdf files
        file_list = []
        for i in os.listdir(homedir):
            if i.split('.')[-1] == 'hdf':
                file_list.append(i)

        for f in file_list:
            hdf = homedir + '/' + f
            hdf_pre = '.'.join(hdf.split('.')[0:3])
            con = pymodis.convertmodis_gdal.convertModisGDAL(hdfname=hdf,
            outformat='GTiff', epsg=29101, subset=[1,1,1], res=250, prefix=hdf_pre)
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
            print "Loading into db: "
            layer = '.'.join(os.path.basename(tif).split('.')[1:3]).lower().replace('.','_')
            os.system("raster2pgsql -C -I {tif} -d {schema}.{layer} \
            | psql patrick".format(tif=tif, schema=schema, layer=layer))

        # Seaching for rasters in the db
        for i in gtif_list:
            if 'b01' in i:
                b_layer = '.'.join(os.path.basename(i).split('.')[1:3]).lower().replace('.','_')

        srch_layer = '_'.join(b_layer.split('_')[0:2])
        conn = psycopg2.connect("dbname=patrick")
        cur = conn.cursor()
        conn.autocommit = True
        cur.execute("select r_table_name from raster_columns where \
        r_table_schema = '{schema}' and r_table_name ~ '{srch_layer}'".format(
        schema = schema.lower(), srch_layer = srch_layer))
        db_list = cur.fetchall()
        # assigning rasters to variables
        for i in db_list:
            if i[0].split('_')[-1] == 'b01':
                band1 = i[0]
            elif i[0].split('_')[-1] == 'b02':
                band2 = i[0]
        # creating ndvi output
        ndvi = band1.replace('b01','ndvi')
        # create ndvi table
        cur.execute("create table {schema}.{ndvi} \
        (rid int primary key, rast raster)".format(schema = schema, ndvi = ndvi))

        print "Calculating {modis}.{ndvi}".format(modis=modis, ndvi=ndvi)
        # cacluating ndvi and outputting to ndvi table
        cur.execute("insert into {schema}.{ndvi}(rid,rast) select \
        1,st_mapalgebra(r1.rast,1,r2.rast,1, 'case when [rast2.val] + [rast1.val] = 0 \
        then Null else ([rast2.val] - [rast1.val])/([rast2.val] + [rast1.val])\
        end'::text,'32BF'::text,Null) from {schema}.{band1} as r1, \
        {schema}.{band2} as r2".format(ndvi=ndvi, schema=schema, band1=band1,
         band2=band2))

        # Summerizing ndvi to polygons
        print "Summerizing ndvi"
        cur.execute("insert into ndvi.brazil SELECT uf,regiao, micro, geocodigo, \
        {year}::int as year, '{doy}'::varchar(3) as doy, '{date}'::date as date, \
        '{ndvi}' as image, (stats).* , med FROM (SELECT uf, regiao, geocodigo, \
        micro, ST_SummaryStats(ST_Clip(rast, st_transform(brazil.micro_regions.wkb_geometry, \
        29101))::raster) as stats, ST_Quantile(ST_Clip(rast, \
        st_transform(brazil.micro_regions.wkb_geometry, 29101))::raster, .5) as med \
        from {schema}.{ndvi}, brazil.micro_regions where \
        st_intersects(rast, st_transform(brazil.micro_regions.wkb_geometry,29101))) \
        as foo".format(year=year, doy=doy, date=ed_date, schema=schema, ndvi=ndvi))

        # Cleaning DB
        for tab in [band1, band2, ndvi]:
            print "Dropping {schema}.{tab}"
            cur.execute("select dropgeometrytable('{schema}','{tab}')".format(schema=schema,
             tab=tab))

        #Deleting tif files
        for i in gtif_list:
            print "Deleting " + i
            os.remove(i)
            os.remove(i + '.aux.xml')

process_func(sat, prod, tile, year, st_doy, ed_doy)
