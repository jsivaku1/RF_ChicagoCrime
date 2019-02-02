from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from datetime import *
from dateutil.parser import parse
import pandas as pd
#import pyspark_csv as pycsv
from pyspark.sql.functions import lit, year, dayofmonth, month
import calendar
from time import strftime
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext

#from bokeh.charts import Histogram,Bar, output_file, show
sc = SparkContext()


#for plotting burtin graph
from collections import OrderedDict
from math import log, sqrt

import numpy as np
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from datetime import *
from dateutil.parser import parse
import pandas as pd

#from bokeh.plotting import figure, show, output_file




sqlContext = SQLContext(sc)

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

#####For loading the trip data
df1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/Users/jayanthsivakumar/Desktop/spark-2.1.0-bin-hadoop2.7/samplecrime.csv')
df1 = sqlContext.read.load('/Users/jayanthsivakumar/Desktop/spark-2.1.0-bin-hadoop2.7/samplecrime.csv',                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')


'''
df2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCcrime/trip/trip_data_2.csv')
df2 = sqlContext.read.load('/home/jay/Documents/NYCcrime/trip/trip_data_2.csv',                          format='com.databricks.spark.csv',
                          header='false',
                          inferSchema='true')


df3 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCcrime/trip/trip_data_8.csv')
df3 = sqlContext.read.load('/home/jay/Documents/NYCcrime/trip/trip_data_8.csv',                          format='com.databricks.spark.csv',
                          header='false',
                          inferSchema='true')


df4 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCcrime/trip/trip_data_9.csv')
df4 = sqlContext.read.load('/home/jay/Documents/NYCcrime/trip/trip_data_9.csv',                          format='com.databricks.spark.csv',
                          header='false',
                          inferSchema='true')

df5 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCcrime/trip/trip_data_10.csv')
df5 = sqlContext.read.load('/home/jay/Documents/NYCcrime/trip/trip_data_10.csv',                          format='com.databricks.spark.csv',
                          header='false',
                          inferSchema='true')

df6 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCcrime/trip/trip_data_11.csv')
df6 = sqlContext.read.load('/home/jay/Documents/NYCcrime/trip/trip_data_11.csv',                          format='com.databricks.spark.csv',
                          header='false',
                          inferSchema='true')

df7 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCcrime/trip/trip_data_12.csv')
df7 = sqlContext.read.load('/home/jay/Documents/NYCcrime/trip/trip_data_12.csv',                          format='com.databricks.spark.csv',
                          header='false',
                          inferSchema='true')

'''


print df1.show()
#df1 = df1.rdd

#df2 = df2.rdd
'''
df3 = df3.rdd
df4 = df4.rdd
df5 = df5.rdd
df6 = df6.rdd
df7 = df7.rdd
'''
'''
header1 = df1.filter(lambda l: "medallion" in l)
df1 = df1.subtract(header1)
'''


#header2 =df2.filter(lambda l: "medallion" in l)
#df2 = df2.subtract(header2)

'''
header3 =df3.filter(lambda l: "medallion" in l)
df3 = df3.subtract(header3)

header4 =df4.filter(lambda l: "medallion" in l)
df4 = df4.subtract(header4)

header5 =df5.filter(lambda l: "medallion" in l)
df5 = df5.subtract(header5)

header6 =df6.filter(lambda l: "medallion" in l)
df6 = df6.subtract(header6)

header7 =df7.filter(lambda l: "medallion" in l)
df7 = df7.subtract(header7)
'''
#df1 = df1.toDF()
#df2 = df2.toDF()
'''
df3 = df3.toDF()
df4 = df4.toDF()
df5 = df5.toDF()
df6 = df6.toDF()
df7 = df7.toDF()
'''


#df = unionAll(df1, df2, df3, df4, df5, df6, df7)
#df = unionAll(df1, df2)
#df = df.rdd
df = df1

#df1 = sqlContext.createDataFrame(df, ["medallion","hack_license","vendor_id","rate_code","store_and_fwd_flag","pickup_datetime","dropoff_datetime","passenger_count","trip_time_in_secs","trip_distance","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude"])






df1.registerTempTable("Temp")

crime=sqlContext.sql("SELECT Longitude, Latitude from Temp").rdd

crime_loc_temp = sqlContext.createDataFrame(crime, ["long","lat"])




crime_loc_temp.registerTempTable("crimeLocationTemp")


#crime_loc =sqlContext.sql("SELECT * from crimeLocationTemp where long ='null' OR lat ='null'").show(720)
#crime_loc = [StructField(field_name, StringType(), True) for field_name in crime_loc_temp.first()]
#crime_loc[0].dataType = FloatType()
#crime_loc[1].dataType = FloatType()



crime_loc_temp.registerTempTable("crimeLocation")

#crime_loc = crime_loc_temp.dropna()





#print ("########################Long and lat for all the data")
latlong = sqlContext.sql("SELECT long, lat FROM crimeLocation").toPandas()

latlong[['long','lat']] = latlong[['long','lat']].astype(float)





#def f(latlong):
#	print(latlong.long,latlong.lat)

#latlong.foreach(f)
#print latlong
#latlong.select("long")
#print latlong.long.rdd.flatMap(list).first()
'''

'''

#for x in latlong.iterrows():
#	print x[0],x[1]
'''
disp=latlong.select("long").collect()

#latlong["long"][2].show()

#disp=sqlContext.sql("long")
print len(disp)

disp2=latlong.select("lat").collect()

print latlong.long
'''

class pygmaps:

	def __init__(self, centerLat, centerLng, zoom ):
		self.center = (float(centerLat),float(centerLng))
		self.zoom = int(zoom)
		#self.grids = None
		#self.paths = []
		self.points = []
		#self.radpoints = []
		self.gridsetting = None
		self.coloricon = 'http://chart.apis.google.com/chart?cht=mm&chs=12x16&chco=FFFFFF,XXXXXX,000000&ext=.png'

	# def setgrids(self,slat,elat,latin,slng,elng,lngin):
	# 	self.gridsetting = [slat,elat,latin,slng,elng,lngin]

	def addpoint(self, lat, lng, color = '#FF0000'):
		self.points.append((lat,lng,color[1:]))

	#def addpointcoord(self, coord):
	#	self.points.append((coord[0],coord[1]))

	#def addradpoint(self, lat,lng,rad,color = '#0000FF'):
	#	self.radpoints.append((lat,lng,rad,color))

	#def addpath(self,path,color = '#FF0000'):
	#	path.append(color)
	#	self.paths.append(path)

	#create the html file which inlcude one google map and all points and paths
	def draw(self, htmlfile):
		f = open(htmlfile,'w')
		f.write('<html>\n')
		f.write('<head>\n')
		f.write('<meta name="viewport" content="initial-scale=1.0, user-scalable=no" />\n')
		f.write('<meta http-equiv="content-type" content="text/html; charset=UTF-8"/>\n')
		f.write('<title>Google Maps - pygmaps </title>\n')
		f.write('<script type="text/javascript" src="http://maps.google.com/maps/api/js?sensor=false"></script>\n')
		f.write('<script type="text/javascript">\n')
		f.write('\tfunction initialize() {\n')
		self.drawmap(f)
		#self.drawgrids(f)
		self.drawpoints(f)
		#self.drawradpoints(f)
		#self.drawpaths(f,self.paths)
		f.write('\t}\n')
		f.write('</script>\n')
		f.write('</head>\n')
		f.write('<body style="margin:0px; padding:0px;" onload="initialize()">\n')
		f.write('\t<div id="map_canvas" style="width: 100%; height: 100%;"></div>\n')
		f.write('</body>\n')
		f.write('</html>\n')
		f.close()

	# def drawgrids(self, f):
	# 	if self.gridsetting == None:
	# 		return
	# 	slat = self.gridsetting[0]
	# 	elat = self.gridsetting[1]
	# 	latin = self.gridsetting[2]
	# 	slng = self.gridsetting[3]
	# 	elng = self.gridsetting[4]
	# 	lngin = self.gridsetting[5]
	# 	self.grids = []

	# 	r = [slat+float(x)*latin for x in range(0, int((elat-slat)/latin))]
	# 	for lat in r:
	# 		self.grids.append([(lat+latin/2.0,slng+lngin/2.0),(lat+latin/2.0,elng+lngin/2.0)])

	# 	r = [slng+float(x)*lngin for x in range(0, int((elng-slng)/lngin))]
	# 	for lng in r:
	# 		self.grids.append([(slat+latin/2.0,lng+lngin/2.0),(elat+latin/2.0,lng+lngin/2.0)])

	# 	for line in self.grids:
	# 		self.drawPolyline(f,line,strokeColor = "#000000")
	def drawpoints(self,f):
		for point in  self.points:
			self.drawpoint(f,point[0],point[1],point[2])

	# def drawradpoints(self, f):
	# 	for rpoint in self.radpoints:
	# 		path = self.getcycle(rpoint[0:3])
	# 		self.drawPolygon(f,path,strokeColor = rpoint[3])

	def getcycle(self,rpoint):
		cycle = []
		lat = rpoint[0]
		lng = rpoint[1]
		rad = rpoint[2] #unit: meter
		d = (rad/1000.0)/6378.8;
		lat1 = (math.pi/180.0)* lat
		lng1 = (math.pi/180.0)* lng

		r = [x*30 for x in range(12)]
		for a in r:
			tc = (math.pi/180.0)*a;
			y = math.asin(math.sin(lat1)*math.cos(d)+math.cos(lat1)*math.sin(d)*math.cos(tc))
			dlng = math.atan2(math.sin(tc)*math.sin(d)*math.cos(lat1),math.cos(d)-math.sin(lat1)*math.sin(y))
			x = ((lng1-dlng+math.pi) % (2.0*math.pi)) - math.pi
			cycle.append( ( float(y*(180.0/math.pi)),float(x*(180.0/math.pi)) ) )
		return cycle

	#def drawpaths(self, f, paths):
	#	for path in paths:
			#print path
	#		self.drawPolyline(f,path[:-1], strokeColor = path[-1])

	#############################################
	# # # # # # Low level Map Drawing # # # # # #
	#############################################
	def drawmap(self, f):
		f.write('\t\tvar centerlatlng = new google.maps.LatLng(%f, %f);\n' % (self.center[0],self.center[1]))
		f.write('\t\tvar myOptions = {\n')
		f.write('\t\t\tzoom: %d,\n' % (self.zoom))
		f.write('\t\t\tcenter: centerlatlng,\n')
		f.write('\t\t\tmapTypeId: google.maps.MapTypeId.ROADMAP\n')
		f.write('\t\t};\n')
		f.write('\t\tvar map = new google.maps.Map(document.getElementById("map_canvas"), myOptions);\n')
		f.write('\n')



	def drawpoint(self,f,lat,lon,color):
		f.write('\t\tvar latlng = new google.maps.LatLng(%f, %f);\n'%(lat,lon))
		f.write('\t\tvar img = new google.maps.MarkerImage(\'%s\');\n' % (self.coloricon.replace('XXXXXX',color)))
		f.write('\t\tvar marker = new google.maps.Marker({\n')
		f.write('\t\ttitle: "no implimentation",\n')
		f.write('\t\ticon: img,\n')
		f.write('\t\tposition: latlng\n')
		f.write('\t\t});\n')
		f.write('\t\tmarker.setMap(map);\n')
		f.write('\n')

	def drawPolyline(self,f,path,\
			clickable = False, \
			geodesic = True,\
			strokeColor = "#FF0000",\
			strokeOpacity = 1.0,\
			strokeWeight = 2
			):
		f.write('var PolylineCoordinates = [\n')
		for coordinate in path:
			f.write('new google.maps.LatLng(%f, %f),\n' % (coordinate[0],coordinate[1]))
		f.write('];\n')
		f.write('\n')

		f.write('var Path = new google.maps.Polyline({\n')
		f.write('clickable: %s,\n' % (str(clickable).lower()))
		f.write('geodesic: %s,\n' % (str(geodesic).lower()))
		f.write('path: PolylineCoordinates,\n')
		f.write('strokeColor: "%s",\n' %(strokeColor))
		f.write('strokeOpacity: %f,\n' % (strokeOpacity))
		f.write('strokeWeight: %d\n' % (strokeWeight))
		f.write('});\n')
		f.write('\n')
		f.write('Path.setMap(map);\n')
		f.write('\n\n')

	def drawPolygon(self,f,path,\
			clickable = False, \
			geodesic = True,\
			fillColor = "#000000",\
			fillOpacity = 0.0,\
			strokeColor = "#FF0000",\
			strokeOpacity = 1.0,\
			strokeWeight = 1
			):
		f.write('var coords = [\n')
		for coordinate in path:
			f.write('new google.maps.LatLng(%f, %f),\n' % (coordinate[0],coordinate[1]))
		f.write('];\n')
		f.write('\n')

		f.write('var polygon = new google.maps.Polygon({\n')
		f.write('clickable: %s,\n' % (str(clickable).lower()))
		f.write('geodesic: %s,\n' % (str(geodesic).lower()))
		f.write('fillColor: "%s",\n' %(fillColor))
		f.write('fillOpacity: %f,\n' % (fillOpacity))
		f.write('paths: coords,\n')
		f.write('strokeColor: "%s",\n' %(strokeColor))
		f.write('strokeOpacity: %f,\n' % (strokeOpacity))
		f.write('strokeWeight: %d\n' % (strokeWeight))
		f.write('});\n')
		f.write('\n')
		f.write('polygon.setMap(map);\n')
		f.write('\n\n')



if __name__ == "__main__":

	########## CONSTRUCTOR: pygmaps(latitude, longitude, zoom) ##############################
	# DESC:		initialize a map  with latitude and longitude of center point
	#		and map zoom level "15"
	# PARAMETER1:	latitude (float) latittude of map center point
	# PARAMETER2:	longitude (float) latittude of map center point
	# PARAMETER3:	zoom (int)  map zoom level 0~20
	# RETURN:	the instant of pygmaps
	#========================================================================================
	mymap = pygmaps(41.881832, -87.623177,10)


	########## FUNCTION: setgrids(start-Lat, end-Lat, Lat-interval, start-Lng, end-Lng, Lng-interval) ######
	# DESC:		set grids on map
	# PARAMETER1:	start-Lat (float), start (minimum) latittude of the grids
	# PARAMETER2:	end-Lat (float), end (maximum) latittude of the grids
	# PARAMETER3:	Lat-interval (float)  grid size in latitude
	# PARAMETER4:	start-Lng (float), start (minimum) longitude of the grids
	# PARAMETER5:	end-Lng (float), end (maximum) longitude of the grids
	# PARAMETER6:	Lng-interval (float)  grid size in longitude
	# RETURN:	no returns
	#========================================================================================
	# mymap.setgrids(37.42, 37.43, 0.001, -122.15, -122.14, 0.001)


	########## FUNCTION:  addpoint(latitude, longitude, [color])#############################
	# DESC:		add a point into a map and dispaly it, color is optional default is red
	# PARAMETER1:	latitude (float) latitude of the point
	# PARAMETER2:	longitude (float) longitude of the point
	# PARAMETER3:	color (string) color of the point showed in map, using HTML color code
	#		HTML COLOR CODE:  http://www.computerhope.com/htmcolor.htm
	#		e.g. red "#FF0000", Blue "#0000FF", Green "#00FF00"
	# RETURN:	no return
	#========================================================================================

	#latitude_arr=[40.7128,40.7500,40.7304]
	#longitude_arr=[-74.0059,-74.0059,-74.0059]

	for i in range(len(latlong)):
			mymap.addpoint(float(latlong.lat[i]),float(latlong.long[i]), "#ff0000")
			#mymap.addpoint(float(latlong.drp_lat[i]),float(latlong.drp_long[i]), "#0000FF")



	#mymap.addpoint(latitude_arr[1], longitude_arr[1], "#ff0000")
	########## FUNCTION:  addradpoint(latitude, longitude, radius, [color])##################
	# DESC: 	add a point with a radius (Meter) - Draw cycle
	# PARAMETER1:	latitude (float) latitude of the point
	# PARAMETER2:	longitude (float) longitude of the point
	# PARAMETER3:	radius (float), radius  in meter
	# PARAMETER4:	color (string) color of the point showed in map, using HTML color code
	#		HTML COLOR CODE:  http://www.computerhope.com/htmcolor.htm
	#		e.g. red "#FF0000", Blue "#0000FF", Green "#00FF00"
	# RETURN:	no return
	#========================================================================================
	## mymap.addradpoint(37.429, -122.145, 95, "#FF0000")


	########## FUNCTION:  addpath(path,[color])##############################################
	# DESC:		add a path into map, the data struceture of Path is a list of points
	# PARAMETER1:	path (list of coordinates) e.g. [(lat1,lng1),(lat2,lng2),...]
	# PARAMETER2:	color (string) color of the point showed in map, using HTML color code
	#		HTML COLOR CODE:  http://www.computerhope.com/htmcolor.htm
	#		e.g. red "#FF0000", Blue "#0000FF", Green "#00FF00"
	# RETURN:	no return
	#========================================================================================
	#path = [(37.429, -122.145),(37.428, -122.145),(37.427, -122.145),(37.427, -122.146),(37.427, -122.146)]
	# mymap.addpath(path,"#00FF00")

	########## FUNCTION:  addpath(file)######################################################
	# DESC:		create the html map file (.html)
	# PARAMETER1:	file (string) the map path and file
	# RETURN:	no return, generate html file in specified directory
	#========================================================================================
	mymap.draw('./mymap.html')
