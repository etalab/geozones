from flask import Flask, render_template, Response, json, current_app, abort

from geozones import geojson
from geozones.model import root

app = Flask(__name__)


def jsonify(data):
    return Response(json.dumps(data), mimetype='application/json')


def stream(data):
    return Response(geojson.stream_zones(data), content_type='application/json')


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


@app.route('/levels/<string:level>')
@app.route('/levels/<string:level>@<string:at>')
def level_at_api(level, at=None):
    db = current_app.db
    data = db.level(level, at)
    return stream(data)


@app.route('/zones/<string:id>')
def zone_api(id):
    db = current_app.db
    zone = db.find_one({'_id': id})
    if zone:
        return jsonify(geojson.zone_to_feature(zone))
    else:
        abort(404)


def run(db, host='localhost', port=5000, debug=False):
    app.db = db
    app.run(host=host, port=port, debug=debug)
