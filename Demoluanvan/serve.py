import time, sys, cherrypy
from paste.translogger import TransLogger
from app import create_app
from pyspark.sql import SparkSession
def init_sparksession():
        spark = SparkSession \
        .builder \
        .appName("Python Spark RFM example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
        return spark

def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5560,
        'server.socket_host': '0.0.0.0'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == "__main__":
    # Init spark context and load libraries
    spark = init_sparksession()
    app = create_app(spark)
 
    # start web server
    run_server(app)