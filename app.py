"""
pixel Service

Example used to show how easy it is to create a simple microservice
using Python and Flask that leverages a mysql database
"""
from datetime import datetime
import os
import logging

import flask
from flaskext.mysql import MySQL
from flask import request
from flask import url_for
from flask import jsonify
from flask import Flask

######################################################################
# Get bindings from the environment
######################################################################
DEBUG = os.getenv("DEBUG", "False") == "True"
# THIS IS OUR DEFAULT PORT
# make sure that port 5000 is exposed in the Vagrant file
PORT = "5000"

######################################################################
# Create Flask application
######################################################################
app = Flask(__name__)
app.logger.setLevel(logging.INFO)


######################################################################
# Application Routes
######################################################################
@app.route("/")
def index():
    """ Returns a message about the service """
    app.logger.info("Request for Index page")
    return (
        jsonify(
            name="IP Data Service", version="1.0", url=url_for("pixel", _external=True)
        ),
        200,
    )


def __insert_records(data: dict):
    try:
        # create mysql connection
        conn = mysql.connect()
        cursor = conn.cursor()

        # updating some string fields
        parameterized_sql = "INSERT INTO pixel_data ( date, useragent, ip, thirdpartyid ) VALUES ( %s, %s, %s, %s );"

        # create data
        prepared_data = (data['date'], data['useragent'], data['ip'], data['thirdpartyid'])
        cursor.execute(parameterized_sql, prepared_data)
        conn.commit()
    except ConnectionError as error:
        error_message = "Cannot contact mysql service: {}".format(error)
        app.logger.error(error_message)
        raise error
    except mysql.connector.Error as error:
        error_message = "parameterized query failed {}".format(error)
        app.logger.error(error_message)
        raise error
    finally:
        conn.close()


@app.route("/pixel", methods=['POST'])
def pixel() -> (dict, int):
    """ Increments the counter each time it is called """
    app.logger.info("Request to increment the counter")
    if flask.request.method == 'POST':
        __insert_records(flask.request.get_json())
    elif flask.request.method == 'GET':
        app.logger.info(request.args.to_dict())
        __insert_records(request.args.to_dict())

    app.logger.info("data inserted")
    return jsonify({"status_code": 200}), 200


######################################################################
#   M A I N
######################################################################
if __name__ == "__main__":
    app.logger.info("*" * 70)
    app.logger.info("   P I X E L   S E R V I C E   ".center(70, "*"))
    app.logger.info("*" * 70)
    mysql = MySQL()
    # hey these are hardcoded...
    app.config['MYSQL_DATABASE_USER'] = 'vagrant'
    app.config['MYSQL_DATABASE_PASSWORD'] = 'password'
    app.config['MYSQL_DATABASE_DB'] = 'cs1660'
    app.config['MYSQL_DATABASE_HOST'] = 'localhost'
    mysql.init_app(app)
    app.run(host="0.0.0.0", port=int(PORT), debug=DEBUG)