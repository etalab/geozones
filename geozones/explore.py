from flask import Flask, render_template, Response, json, current_app

from geozones import geojson
from geozones.model import root

TIMED_LEVELS = ('fr:region', 'fr:epci', 'fr:departement', 'fr:commune')

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
    db = current_app.db
    if level_id in TIMED_LEVELS:
        data = db.fetch_zones(level_id, after='2017-01-01')
    else:
        data = db.find({'level': level_id})
    return Response(geojson.stream_zones(data),
                    content_type='application/json')


def run(db, host='localhost', port=5000, debug=False):
    app.db = db
    app.run(host=host, port=port, debug=debug)
