# save this as app.py
from flask import Flask , jsonify ,request
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import requests
import os
from dotenv import load_dotenv
import datetime

load_dotenv()
app = Flask(__name__)
print(os.environ.get('DATABASE_URI'))
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URI')
db = SQLAlchemy(app)
ma  = Marshmallow(app)


class Transtions(db.Model):
    __tablename__ = "transtions"
    id = db.Column(db.Integer, primary_key=True)
    account_no = db.Column(db.BigInteger, nullable=True)
    date = db.Column(db.DateTime, nullable=True)
    date_in_str = db.Column(db.String, nullable=True)
    transaction_details = db.Column(db.Text, nullable=True)
    value_date = db.Column(db.String, nullable=True)
    withdrawal_amt = db.Column(db.String , nullable = True)
    deposit_amt = db.Column(db.String , nullable = True)
    balance_amt = db.Column(db.String , nullable = True)

    def __init__(self ,account_no,date,date_in_str,transaction_details,value_date,withdrawal_amt,deposit_amt,balance_amt):
        self.account_no = account_no
        self.date = date
        self.date_in_str= date_in_str
        self.transaction_details = transaction_details 
        self.value_date = value_date 
        self.withdrawal_amt = withdrawal_amt
        self.deposit_amt = deposit_amt 
        self.balance_amt = balance_amt
    
class ProductSchema(ma.Schema):
    class Meta :
        fields = ["id","account_no","date_in_str","transaction_details","value_date","withdrawal_amt","deposit_amt","balance_amt"]

product_schema = ProductSchema()
products_schema =ProductSchema(many=True)


@app.route("/transactions/<date>",methods=["GET"])
def transactions_data(date):
    try:
        date  = datetime.datetime.strptime(date.strip(), '%d-%m-%y')
    except:
        return jsonify({"error":"Date shuld be in dd-mm-yy."})
    all_transtion = Transtions.query.filter((Transtions.date == date))
    result = products_schema.dump(all_transtion)
    return jsonify(result)

@app.route("/balance/<date>",methods=["GET"])
def balance_data(date):
    try:
        date  = datetime.datetime.strptime(date.strip(), '%d-%m-%y')
    except:
        return jsonify({"error":"Date shuld be in dd-mm-yy."})
    all_transtion = Transtions.query.filter((Transtions.date == date)).order_by(Transtions.balance_amt)[0]
    result = product_schema.dump(all_transtion)
    return jsonify(result)

@app.route("/details/<id>",methods=["GET"])
def details(id):
    all_transtion = Transtions.query.get(id)
    result = product_schema.dump(all_transtion)
    return jsonify(result)

@app.route("/add",methods=["POST"])
def add():
    error ={}
    for i in ["account_no","date","transaction_details","value_date","balance_amt"] :
        if not request.json.get(i):
            error[i] =  i + " can't be empty"
    if error:
        return jsonify(error)
    try:
        date  = datetime.datetime.strptime(request.json['date'].strip(), '%d-%m-%y')
    except Exception as e:
        print("eer",e)
        return jsonify({"error":"Date shuld be in dd-mm-yy."})
    new_record = Transtions(account_no = request.json['account_no'],
                            date = date,
                            date_in_str= request.json['date'],
                            transaction_details= request.json['transaction_details'],
                            value_date = request.json['value_date'],
                            withdrawal_amt = request.json.get('withdrawal_amt'),
                            deposit_amt = request.json.get('deposit_amt'),
                            balance_amt  = request.json.get('balance_amt'))
    db.session.add_all(new_record)
    db.session.commit()
    return jsonify(new_record)



@app.route("/load")
def load():
    url = os.environ.get('URL')
    loaddata = requests.get(url).json()
    datalis = []
    for i in loaddata:
        
     
        new_entry = Transtions(account_no = i['Account No'],
                                date = datetime.datetime.strptime(i['Date'].strip(), '%d %b %y'),
                                date_in_str= i['Date'],
                                transaction_details= i['Transaction Details'],
                                value_date = i['Value Date'],
                                withdrawal_amt = i['Withdrawal AMT'],
                                deposit_amt = i['Deposit AMT'],
                                balance_amt  = i['Balance AMT'])
        datalis.append(new_entry)
    db.session.add_all(datalis)
    db.session.commit()
   

    return jsonify({"succes":True})

if __name__ == '__main__':
    db.create_all()
    
    app.run(debug=True)