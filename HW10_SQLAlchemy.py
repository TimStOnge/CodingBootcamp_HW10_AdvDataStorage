import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from datetime import timedelta, datetime

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)
#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

conn = engine.connect()
measurements_df = pd.read_sql("SELECT * FROM measurement", conn)
stations_df = pd.read_sql("SELECT * FROM station", conn)

@app.route("/")
def welcome():
    return (
        f"Welcome to HOMEWORK 10!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start>/<end><br/>"
        f"NOTE: Use /api/v1.0/[Start_Date]/[End_Date] in %Y-%m-%d format."
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    conn = engine.connect()
    measurements_df = pd.read_sql("SELECT * FROM measurement", conn)
    
    
    precip_df = measurements_df.sort_values('date', ascending=False)
    last_date = precip_df['date'].iloc[0]

    # Convert the stored latest date from string to datetime.
    last_date_datetime = datetime.strptime(last_date, '%Y-%m-%d')

    # Now that the latest year in the dataframe is in datetime, calculate that date minus 365 days (using timedelta).
    last_yr_of_data = last_date_datetime - timedelta(days=365)

    # Convert the "Date" column to datetime type with proper formatting and test the result.
    precip_df['date'] = pd.to_datetime(precip_df['date'], format='%Y-%m-%d')

    # Filter from the dataframe all records where the "date" is after the {last_yr_of_data} variable (which indicates...
    # ...one year before the end of the dataset).
    precip_lastyr_df = precip_df[(precip_df['date'] >= last_yr_of_data)]

    # Convert the data in the "date" and "prcp" dataframe columns into respective lists.
    all_dates = precip_lastyr_df['date'].tolist()
    all_precip = precip_lastyr_df['prcp'].tolist()
    
    dates_list_str = [datetime.strftime(date, "%Y-%m-%d") for date in all_dates]

    precip_dict = dict(zip(dates_list_str, all_precip))

    """Return the justice league data as json"""

    return jsonify(precip_dict)


@app.route("/api/v1.0/stations")
def stations():
    conn = engine.connect()
    stations_df = pd.read_sql("SELECT * FROM station", conn)
    
    stations_list = stations_df['name'].tolist()
    
    return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
def tobs():
    conn = engine.connect()
    measurements_df = pd.read_sql("SELECT * FROM measurement", conn)
    
    temp_df = measurements_df.sort_values('date', ascending=False)
    last_date = temp_df['date'].iloc[0]

    # Convert the stored latest date from string to datetime.
    last_date_datetime = datetime.strptime(last_date, '%Y-%m-%d')

    # Now that the latest year in the dataframe is in datetime, calculate that date minus 365 days (using timedelta).
    last_yr_of_data = last_date_datetime - timedelta(days=365)

    # Convert the "Date" column to datetime type with proper formatting and test the result.
    temp_df['date'] = pd.to_datetime(temp_df['date'], format='%Y-%m-%d')

    # Filter from the dataframe all records where the "date" is after the {last_yr_of_data} variable (which indicates...
    # ...one year before the end of the dataset).
    temp_lastyr_df = temp_df[(temp_df['date'] >= last_yr_of_data)]
    
    all_dates = temp_lastyr_df['date'].tolist()
    all_temp = temp_lastyr_df['tobs'].tolist()
    
    dates_list_str = [datetime.strftime(date, "%Y-%m-%d") for date in all_dates]

    temp_dict = dict(zip(dates_list_str, all_temp))
    
    return jsonify(temp_dict)


@app.route("/api/v1.0/<start_date>/<end_date>")
def start_end(start_date, end_date):
    conn = engine.connect()
    measurements_df = pd.read_sql("SELECT * FROM measurement", conn)
    
    start_date_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_datetime = datetime.strptime(end_date, '%Y-%m-%d')
    
    temp_df = measurements_df.sort_values('date', ascending=False)
   
    temp_df['date'] = pd.to_datetime(temp_df['date'], format='%Y-%m-%d')


    temp_after_begin = temp_df[(temp_df['date'] >= start_date_datetime)]
    
    temp_before_end = temp_after_begin[(temp_after_begin['date'] <= end_date_datetime)]
    
    
    temp_before_end_list = temp_before_end['tobs'].tolist()
    
    temp_avg = np.mean(temp_before_end_list)
    temp_min = np.min(temp_before_end_list)
    temp_max = np.max(temp_before_end_list)

    temp_stats = {
		"Temperature Average" : temp_avg,
		"Temperature Minimum" : temp_min,
		"Temperature Maximum" : temp_max,
	}

    
    return jsonify(temp_stats)

#
#
# Stopping flask can be a pain, so I recommend this ONLY in dev
    
#from flask import request
#
#def shutdown_server():
#    func = request.environ.get('werkzeug.server.shutdown')
#    if func is None:
#        raise RuntimeError('Not running with the Werkzeug Server')
#    func()
#    
#@app.route('/shutdown')
#def shutdown():
#    shutdown_server()
#    return 'Server shutting down...'
#
#
if __name__ == "__main__":
    app.run(debug=True)