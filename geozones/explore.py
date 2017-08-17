from flask import Flask, render_template, Response, json

from geozones import geojson
from geozones.db import DB
from geozones.model import root


app = Flask(__name__)


def jsonify(data):
    return Response(json.dumps(data), mimetype='application/json')


@app.route('/')
def frontend():
    return render_template('explore.html')


def level_to_dict(level):
    return {
        'id': level.id,
        'label': level.label,
        'parents': [p.id for p in level.parents]
    }


@app.route('/levels')
def levels_api():
    return jsonify([level_to_dict(l) for l in root.traverse()])


@app.route('/levels/<path:level_id>')
def level_api(level_id):
    db = DB()
    data = geojson.dump_zones(db.find({'level': level_id}))
    return jsonify(data)


def run(debug=False):
    app.run(debug=debug)
