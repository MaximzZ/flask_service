from datetime import datetime

from flask import Flask

from flask_sqlalchemy import SQLAlchemy
import pandas as pd


app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:forgettt@localhost/sber_db'
db = SQLAlchemy(app)


class DataTable(db.Model):
    __tablename__ = 'Data_Table'

    id = db.Column(db.Integer, primary_key=True)
    Rep_dt = db.Column(db.Date)
    Delta = db.Column(db.Float)

db.create_all()


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
    return 'Hello World!'


@app.route('/import/xlsx')
def read_file():
    df = pd.read_excel('testData.xlsx', converters={'Delta': correct_number, 'Rep_dt': get_date})
    df.to_sql(DataTable.__tablename__, db.engine, if_exists='replace', index=False, dtype={'Delta': db.Float, 'Rep_dt': db.Date})

    return 'Ok'


# @app.route('/export/sql')
# def export_sql():



if __name__ == '__main__':
    app.run(debug=True)
