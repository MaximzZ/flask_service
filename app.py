from datetime import datetime
import json

from flask import Flask, request

from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import numpy as np


app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:forgettt@localhost:5433/sber_db'
db = SQLAlchemy(app)
db.Model.metadata.reflect(bind=db.engine,schema='sber_db',views=True)


class DataTable(db.Model):
    __tablename__ = 'Data_Table'

    id = db.Column(db.Integer, primary_key=True)
    Rep_dt = db.Column(db.Date)
    Delta = db.Column(db.Float)


db.create_all()


class DeltaView(db.Model):
    __tablename__ = 'DeltaView'

    id = db.Column(db.Integer, primary_key=True)
    Rep_dt = db.Column(db.Date)
    Delta = db.Column(db.Float)
    DeltaLag = db.Column(db.TIMESTAMP)


def correct_number(number):
    if type(number) == str:
        number = number.replace(',', '.')
        return float(number)
    else:
        return number


def get_date(date_string):
    locale_formats = ['%d.%m.%Y', '%Y-%m-%d']
    for format_string in locale_formats:
        try:
            return datetime.strptime(date_string, format_string)
        except ValueError:
            continue


@app.route('/')
def index():
    return {"result": "Service", "resultStr": "OK"}


@app.route('/import/xlsx')
def read_file():
    DataTable.query.delete()
    db.session.commit()
    df = pd.read_excel('testData.xlsx', converters={'Delta': correct_number, 'Rep_dt': get_date})
    df.to_sql(DataTable.__tablename__, db.engine, if_exists='append', index_label='id', dtype={'Delta': db.Float, 'Rep_dt': db.Date})

    return '{"result": "Xlsx loaded", "resultStr": "OK"}'



@app.route('/export/sql')
def export_sql():
    '''
     SELECT "Data_Table".id,
    "Data_Table"."Rep_dt",
    "Data_Table"."Delta",
    "Data_Table"."Rep_dt" - '2 mons'::interval AS "DeltaLag"
   FROM "Data_Table"
    '''
    req_result = DeltaView.query.all()
    result = []
    for row in req_result:
        result.append({'rep_dt': str(row.Rep_dt), 'Delta': row.Delta, 'DeltaLag': str(row.DeltaLag.date())})
    result_str = json.dumps(result)
    return f'{{"result": {result_str}, "resultStr": "OK"}}'


@app.route('/export/pandas')
def export_pandas():
    delta = int(request.args.get('delta'))
    df = pd.read_sql(DataTable.__tablename__, db.engine, index_col='id', parse_dates=['Rep_dt'])
    df['Rep_dt'] = df['Rep_dt'].dt.date
    df['DeltaLag'] = df['Rep_dt'] - np.timedelta64(delta, 'M')

    # df.to_json(orient='records', date_format='iso')
    result = []
    for index, row in df.iterrows():
        result.append({'rep_dt': str(row.Rep_dt), 'Delta': row.Delta, 'DeltaLag': str(row.DeltaLag)})
    result_str = json.dumps(result)
    return f'{{"result": {result_str}, "resultStr": "OK"}}'


if __name__ == '__main__':
    app.run(debug=True)
