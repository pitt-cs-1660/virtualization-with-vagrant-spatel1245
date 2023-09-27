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


def __delete_records():
    try:
        # create mysql connection
        conn = mysql.connect()
        cursor = conn.cursor()

        # get all records in the database and return them
        cursor.execute('truncate pixel_data')
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


def __get_records() -> any:
    try:
        # create mysql connection
        conn = mysql.connect()
        cursor = conn.cursor()

        # get all records in the database and return them
        cursor.execute('select * from pixel_data')
        res = cursor.fetchall()
        conn.commit()
        return res
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
        app.logger.info(prepared_data)
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


@app.route("/pixel", methods=['POST', 'GET', 'DELETE'])
def pixel() -> (dict, int):
    data = ""
    """ Increments the counter each time it is called """
    if flask.request.method == 'POST':
        app.logger.info("Request to save pixel data")
        __insert_records(flask.request.get_json())
    elif flask.request.method == 'GET':
        app.logger.info("Request to get pixel data")
        data = __get_records()
    elif flask.request.method == 'DELETE':
        app.logger.info("Request to delete pixel data")
        __delete_records()

    app.logger.info("data inserted")
    return jsonify({"status_code": 200, "data": data}), 200


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