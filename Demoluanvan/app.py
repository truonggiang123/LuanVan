from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
from flask import Flask, request
from flask_cors import CORS, cross_origin
from flask import jsonify

@main.route("/ratingforuser", methods=["GET"])
@cross_origin()
def ratingforuser():
    trieuchung = request.args.get('trieuchung')
    top_ratings = recommendation_engine.get_movie_recomment_from_user(trieuchung)
    pandas_dataframe = top_ratings.toPandas()
    result = pandas_dataframe.to_json(orient='records')
    return result
    
@main.route("/ratingforloaibenh", methods=["GET"])
@cross_origin()
def ratingforloaibenh():
    loaibenh = request.args.get('loaibenh')
    top_ratings = recommendation_engine.get_thuoc_recomment_from_loaibenh(loaibenh)
    pandas_dataframe = top_ratings.toPandas()
    result = pandas_dataframe.to_json(orient='records')
    return result
    
def create_app(spark_session):
    global recommendation_engine 
 
    recommendation_engine = RecommendationEngine(spark_session)    
    
    app = Flask(__name__)
    cors = CORS(app)
    app.config['CORS_HEADERS'] = 'Content-Type'
    app.register_blueprint(main)
    return app