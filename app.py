import numpy as np
import pandas as pd
import datetime as dt
from flask import Flask, jsonify

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

engine = create_engine("sqlite:///Resources/hawaii.sqlite", connect_args={'check_same_thread': False}, echo=True)

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# We can view all of the classes that automap found
Base.classes.keys()

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


# Query last data point in the database
last_date = str(session.query(Measurement.date).order_by(Measurement.date.desc()).first())

# Calculate the date 1 year ago from the last data point in the database
last_year = (dt.datetime.strptime(last_date, "('%Y-%m-%d',)") - dt.timedelta(days = 365))

# Design a query to retrieve the last 12 months of precipitation data
rain = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= last_year.date()).order_by(Measurement.date).all()

# Save the query results as a Pandas DataFrame and set the index to the date column
# Sort the dataframe by date
rain_df = pd.DataFrame(rain).sort_values("date").set_index("date")

# Rename column
rain_df = rain_df.rename(columns={"prcp": "precipitation"})

# List the stations and the counts in descending order.
list_by_activity = session.query(Measurement.station,func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()

# Most active station based on # of rows is the first on the list.
most_active = list_by_activity[0][0]

# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature of most active station?

min_temp = session.query(func.min(Measurement.tobs)).filter(Measurement.station == most_active).order_by(Measurement.station).all()
max_temp = session.query(func.max(Measurement.tobs)).filter(Measurement.station == most_active).order_by(Measurement.station).all()
ave_temp = session.query(func.avg(Measurement.tobs)).filter(Measurement.station == most_active).order_by(Measurement.station).all()

print(min_temp, max_temp, ave_temp)


# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station

tobs = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= last_year.date(), Measurement.station == most_active).all()

tobs_df = pd.DataFrame(tobs).set_index("date")
tobs_df.head()

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################


@app.route("/")
def homepage():
    return(
        f"Available Routes:<br/>"
        f"(Note: Available date is from 2010-01-01 to 2017-08-23.)<br/>"
		f"<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"- Return dates and precipitation from the last year.<br/>"
		f"<br/>"
        f"/api/v1.0/stations<br/>"
        f"- Returns a json list of stations<br/>"
		f"<br/>"
        f"/api/v1.0/tobs<br/>"
        f"- Returns list of Temperature Observations(tobs) from the last year. <br/>"
		f"<br/>"
        f"/api/v1.0/yyyy-mm-dd<br/>"
        f"- Returns average, minimum and maximum temperature for all dates greater than and equal to the start date.<br/>"
		f"<br/>"
        f"/api/v1.0/yyyy-mm-dd/yyyy-mm-dd<br/>"
        f"- Returns average, minimum and maximum temperature for dates between the start and end date inclusive.<br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
	prcp_dict = rain_df.to_dict()
	return jsonify(prcp_dict)

@app.route("/api/v1.0/stations")
def stations():
	stations_list = list(np.ravel(list_by_activity))
	return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
def tobs():
    tobs_dict = tobs_df.to_dict()
    return jsonify(tobs_dict)

@app.route("/api/v1.0/<start>")
def start(start):

    temp_start = session.query(func.avg(Measurement.tobs),func.min(Measurement.tobs),func.max(Measurement.tobs)).filter(Measurement.date >= start)

    temp_start_list = list(temp_start)

    return jsonify(temp_start_list)

@app.route("/api/v1.0/<start>/<end>")
def end(start, end):

    temp_between = session.query(func.min(Measurement.tobs),\
                func.avg(Measurement.tobs),\
                func.max(Measurement.tobs)).\
                filter(Measurement.date >= start,\
                    Measurement.date <= end)

    temp_between_list = list(temp_between)

    return jsonify(temp_between_list)

if __name__ == '__main__':
    app.run(debug=True)