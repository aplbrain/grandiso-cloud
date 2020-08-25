from flask import Flask, jsonify

app = Flask(__name__)

import logging


@app.route("/")
def lambda_get_next_candidates():
    Logger = logging.getLogger()
    Logger.setLevel(logging.DEBUG)

    Logger.info("IT'S WORKING")
    return jsonify({"1": 1})
