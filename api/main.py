import os
import docker
import pandas as pd
import sqlalchemy as db
from sqlalchemy import Column, Float, Table, Integer
from sqlalchemy.ext.declarative import declarative_base
from flask import Flask, json, request, Response

app = Flask(__name__)
app.config["DEBUG"] = True

ip = docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' container_name_or_id #state here the name of training container

class DBUtil:

    def __init__(self):
        db_url = curl ip:5000 #I think this is 5000, could be 5001
        # create the database
        self.engine = db.create_engine(db_url, echo=True)
        self._reflect()

    def create_tb(self, table_name, column_names):
        # Get a connection to the database
        conn = self.engine.connect()
        self._reflect()
        # Start the database transaction
        trans = conn.begin()
        # Define the columns of the table. Assume that a list of column names are provided and column type is float.
        columns = (Column(name, Float, quote=False) for name in column_names)
        # Create the table. Id is the primary key and it will be automatically generated.
        v_table = Table(table_name, self.Base.metadata, Column('id', Integer, primary_key=True, autoincrement=True),
                        extend_existing=True, *columns)
        v_table.create(self.engine, checkfirst=True)
        # End the database transaction
        trans.commit()

    def drop_tb(self, table_name):
        conn = self.engine.connect()
        trans = conn.begin()
        self._reflect()
        # get a reference to the table
        v_table = self.Base.metadata.tables[table_name]
        v_table.drop(self.engine, checkfirst=True)
        trans.commit()

    def add_data_records(self, table_name, records):
        self._reflect()
        # get a reference to the table
        v_table = self.Base.metadata.tables[table_name]
        # get a query object for inserting data
        query = db.insert(v_table)
        connection = self.engine.connect()
        trans = connection.begin()
        # run the query
        connection.execute(query, records)
        trans.commit()

    def read_data_records(self, table_name):
        self._reflect()
        v_table = self.Base.metadata.tables[table_name]
        connection = self.engine.connect()
        trans = connection.begin()
        # get a query object for reading all data
        query = db.select([v_table])
        # read all data
        df = pd.read_sql_query(query, con=connection)
        trans.commit()
        return df

    # refresh meta-data
    def _reflect(self):
        self.Base = declarative_base()
        self.Base.metadata.reflect(self.engine)


app = Flask(__name__)
app.config["DEBUG"] = True

db_util = DBUtil()


@app.route('/training-db/<table_name>', methods=['POST'])
def create_table(table_name):
    # get the payload or body
    req_data = request.get_json()
    columns = req_data['columns']
    db_util.create_tb(table_name, columns)
    return json.dumps({'message': 'a table was created'}, sort_keys=False, indent=4), 200


@app.route('/training-db/<table_name>', methods=['DELETE'])
def delete_table(table_name):
    db_util.drop_tb(table_name)
    return json.dumps({'message': 'a table was dropped'}, sort_keys=False, indent=4), 200


@app.route('/training-db/<table_name>', methods=['PUT'])
def update_data(table_name):
    content = request.get_json()
    db_util.add_data_records(table_name, content)
    return json.dumps({'message': 'training data were updated'}, sort_keys=False, indent=4), 200


@app.route('/training-db/<table_name>', methods=['GET'])
def read_data(table_name):
    df = db_util.read_data_records(table_name)
    df = df.drop(columns=['id'])
    resp = Response(df.to_json(orient='records'), status=200, mimetype='application/json')
    return resp

app.run(host='0.0.0.0', port=5000)
