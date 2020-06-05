from flask import Flask, make_response, jsonify, request
import datetime
import logging
import os
import sqlalchemy
from lib.sample_func import SampleFunc

sample_func = SampleFunc()

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

@app.route('/')
def index():
    return jsonify({"key": "value"})

@app.route('/collections', methods=['GET'])
def filter_collections():
    bline_ids = request.args.getlist("brandId")
    season_ids = request.args.getlist("season")
    cities = request.args.getlist("place")
    genres = request.args.getlist("genre")
    sexes = request.args.getlist("gender")
    collections = sample_func.filter_collections(
                                    bline_ids=bline_ids,
                                    season_ids=season_ids,
                                    cities=cities,
                                    genres=genres,
                                    sexes=sexes
                                    )
    return make_response(jsonify({"collections": collections}))

@app.route('/images', methods=['GET'])
def filter_images():
    bline_ids = request.args.getlist("brandId")
    season_ids = request.args.getlist("season")
    cities = request.args.getlist("place")
    genres = request.args.getlist("genre")
    sexes = request.args.getlist("gender")
    item_groups = request.args.getlist("itemGroup")
    materials = request.args.getlist("itemMaterial")
    patterns = request.args.getlist("itemPattern")
    colors = request.args.getlist("itemColor")
    images = sample_func.filter_images(
                bline_ids=bline_ids,
                season_ids=season_ids,
                cities=cities,
                genres=genres,
                sexes=sexes,
                item_groups=item_groups,
                materials=materials,
                patterns=patterns,
                colors=colors
                )
    return make_response(jsonify({"images": images}))

@app.route('/filterItems/<string:model_type>', methods=['GET'])
def get_filter_items(model_type):
    if model_type == "collection":
        filter_items = []
        brands = sample_func.get_brands()
        seasons = sample_func.get_seasons()
        filter_items = brands + seasons
    elif model_type == "image":
        filter_items = []
        brands = sample_func.get_brands()
        seasons = sample_func.get_seasons()
        item_groups = sample_func.get_item_groups()
        colors = sample_func.get_colors()
        materials = sample_func.get_materials()
        patterns = sample_func.get_patterns()
        filter_items = brands + seasons + item_groups + colors + materials + patterns
    return make_response(jsonify({"filterItems": filter_items}))

@app.route('/collection/<string:collection_id>', methods=['GET'])
def get_collection(collection_id):
    collection = sample_func.filter_collections(bline_ids=[collection_id])[0]
    return make_response(jsonify({"collection": collection}))

@app.route('/articles', methods=['GET'])
def get_articles():
    articles = sample_func.filter_articles()
    print(articles)
    return make_response(jsonify({"articles": articles}))

if __name__ == '__main__':
   app.run()
