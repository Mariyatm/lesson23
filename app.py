import os

from flask import Flask, request
from flask_restx import Resource, Namespace, Api


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

query_ns = Namespace('perform_query')


def query_filter(data, value):
    return filter(lambda row: value in row, data)


def query_map(data, value):
    return map(lambda row: row.split()[int(value)], data)


def query_unique(data, value):
    return (row  for row in set(data))


def query_sort(data, value):
    reverse = False
    if value == "desc":
        reverse = True
    return iter(sorted(data, reverse=reverse))


def run_query(data, value, cmd):
    if cmd == "filter":
        return query_filter(data,value)
    if cmd == "map":
        return query_map(data, value)
    if cmd == "sort":
        return query_sort(data, value)
    if cmd == "unique":
        return query_unique(data, value)


@query_ns.route('/')
class PerformQuery(Resource):
    def post(self):
        try:
            req_json = request.json
            file_name = req_json.get('file_name')
            cmd1 = req_json.get('cmd1')
            value1 = req_json.get('value1')
            cmd2 = req_json.get('cmd2')
            value2 = req_json.get('value2')
        except Exception as e:
            return "404"

        try:
            with open(os.path.join(DATA_DIR, file_name), "r") as f:
                res = list(run_query(run_query(f, value1, cmd1), value2, cmd2))
            return app.response_class("\n".join(res), content_type="text/plain")
        except FileNotFoundError:
            return "400"


if __name__ == '__main__':
    app = Flask(__name__)
    api = Api(app)
    api.add_namespace(query_ns)
    app.run(debug=True)