#!/usr/bin/python3
# Downloads modis tile ingest and calculates ndvi
import psycopg2
import sys
import os
import argparse
getopt

sat='MOLT'
prod = 'MOD09Q1.005'
tile = 'h13v11'
year = 2015
st_doy = 1
ed_doy = 365

st_pydate = datetime.datetime.strptime(str(year)+str(st_doy).zfill(3),'%Y%j')

st_date = st_pydate.strftime('%Y-%m-%d')

doy_tuples = tuple(range(st_doy,ed_doy,8))

for d in doy_tuples:
    doy = str(d).zfill(3)
    ed_pydate = datetime.datetime.strptime(year+doy,'%Y%j')
    ed_date = ed_pydate.strftime('%Y-%m-%d')

    pydown = pymodis.downmodis.downModis('/home/ptrierweiler/modis',
    password=None, user='anonymous', url='http://e4ftl01.cr.usgs.gov',
    tiles='h12v10', path='MOLT', product='MOD09Q1.005',
    today=st_date,enddate=ed_date)
