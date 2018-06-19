from datetime import datetime
from hashlib import md5
from time import time
from flask import current_app
from werkzeug.security import generate_password_hash, check_password_hash
from app import db

from sqlalchemy.dialects.mysql import DOUBLE, TINYBLOB, TINYINT


class Contract(db.Model):
    # secId = db.Column(db.String(75) ) # doesn't seem to be used
    secType = db.Column(db.String(25), nullable=True)
    localSymbol = db.Column(db.String(75), primary_key=True) # productId, null if not a derivative
    symbol = db.Column(db.String(50), nullable=True) # underlying
    currency = db.Column(db.String(5), nullable=True)
    exchange = db.Column(db.String(25), nullable=True)
    primaryExchange = db.Column(db.String(25), nullable=True)
    lastTradeDateOrContractMonth = db.Column(db.String(15), nullable=True)
    multiplier = db.Column(db.Numeric(15, 6), nullable=True)
    strike = db.Column(db.Numeric(15, 6), nullable=True)
    right = db.Column(db.String(25), nullable=True) # put / call

    executions = db.relationship('Execution', backref='asset', lazy='dynamic')

    bbg = db.relationship("Bbg", uselist=False, back_populates="contract")

    def __repr__(self):
        return '<Contract {}>'.format(self.localSymbol)

class Bbg(db.Model):
    ticker = db.Column(db.String(75))
    bbgIdentifier = db.Column(db.String(50), nullable=True)
    bbgUnderylingId = db.Column(db.String(50), nullable=True)
    internalUnderlying = db.Column(db.String(50), nullable=True)

    contract_localSymbol = db.Column(db.String(75), db.ForeignKey('contract.localSymbol'), primary_key=True)
    contract = db.relationship("Contract", back_populates="bbg")

    def __repr__(self):
        return '<Bbg {} - {}>'.format(self.contract_localSymbol, self.ticker)


class Execution(db.Model):
    execId = db.Column(db.String(140), primary_key=True)
    orderId = db.Column(db.BigInteger)

    contract_localSymbol = db.Column(db.String(75), db.ForeignKey('contract.localSymbol'))
    contract = db.relationship("Contract", back_populates="executions")

    time = db.Column(db.DateTime)
    acctNumber = db.Column(db.String(25))
    exchange = db.Column(db.String(25))
    side = db.Column(db.String(15))
    shares = db.Column(db.BigInteger) # execQty
    cumQty = db.Column(db.BigInteger)
    #price = db.Column(DOUBLE(asdecimal=True))
    #avgPrice = db.Column(DOUBLE(asdecimal=True))
    price = db.Column(db.Numeric(15, 6))
    avgPrice = db.Column(db.Numeric(15, 6))
    permId = db.Column(db.BigInteger)


    def __repr__(self):
        return '<Execution {}>'.format(self.execId)
