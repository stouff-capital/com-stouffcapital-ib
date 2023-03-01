from datetime import datetime
from hashlib import md5
from time import time
from flask import current_app
from werkzeug.security import generate_password_hash, check_password_hash
from app import db

from sqlalchemy.dialects.mysql import DOUBLE, TINYBLOB, TINYINT


class Ibcontract(db.Model): # feed static data from daily statement reports
    assetCategory = db.Column(db.String(25), nullable=True)
    symbol = db.Column(db.String(50), nullable=True) # derviative / underlying
    description = db.Column(db.String(75), nullable=True) # for human only, don't use for mapping
    conid = db.Column(db.BigInteger, primary_key=True)
    isin = db.Column(db.String(25), nullable=True)
    listingExchange = db.Column(db.String(25), nullable=True)
    underlyingConid = db.Column(db.Numeric(15, asdecimal=False), nullable=True)
    underlyingSymbol = db.Column(db.String(50), nullable=True)
    underlyingSecurityID = db.Column(db.String(25), nullable=True)
    underlyingListingExchange = db.Column(db.String(25), nullable=True)
    multiplier = db.Column(db.Numeric(15, 6), nullable=True)
    strike = db.Column(db.Numeric(15, 6), nullable=True)
    expiry = db.Column(db.Date(), nullable=True) # derivative
    putCall = db.Column(db.String(10), nullable=True)
    maturity = db.Column(db.Date(), nullable=True) # for bonds
    issueDate = db.Column(db.Date(), nullable=True)
    underlyingCategory = db.Column(db.String(25), nullable=True) # in reports but not in exec
    subCategory = db.Column(db.String(25), nullable=True) # report only
    currency = db.Column(db.String(5), nullable=True) # not available in reports (SecuritiesInfo) but in exec

    ibexecutionrestfuls = db.relationship('Ibexecutionrestful', backref='ibasset', lazy='dynamic') # expose bridge 'ibasset' in Ibexecutionrestful

    bloom = db.relationship("Ibsymbology", uselist=False, back_populates="ibcontract")

    def __repr__(self):
        return f'<Ibcontract:: {self.conid} {self.assetCategory} {self.multiplier} {self.symbol}>'


class Ibexecutionrestful(db.Model):
    # exec
    execution_m_acctNumber = db.Column(db.String(25))
    execution_m_orderId = db.Column(db.BigInteger)
    execution_m_time = db.Column(db.DateTime)
    execution_m_permId = db.Column(db.BigInteger, nullable=True)
    execution_m_price = db.Column(db.Numeric(15, 6))
    execution_m_avgPrice = db.Column(db.Numeric(15, 6), nullable=True)
    execution_m_cumQty = db.Column(db.BigInteger, nullable=True)
    execution_m_side = db.Column(db.String(15))
    execution_m_clientId = db.Column(db.Integer, nullable=True)
    execution_m_shares = db.Column(db.BigInteger) # execQty
    execution_m_execId = db.Column(db.String(140), primary_key=True)
    execution_m_exchange = db.Column(db.String(25), nullable=True)

    # contract
    contract_m_tradingClass = db.Column(db.String(15), nullable=True)
    contract_m_symbol = db.Column(db.String(50)) # underlying
    contract_m_conId = db.Column(db.BigInteger, nullable=False)
    contract_m_secType = db.Column(db.String(25), nullable=True)
    contract_m_right = db.Column(db.String(25), nullable=True) # put / call
    contract_m_multiplier = db.Column(db.Numeric(15, 6), nullable=True)
    contract_m_expiry = db.Column(db.Date(), nullable=True) # derivative
    contract_m_localSymbol = db.Column(db.String(75), nullable=False) # symbol for derivative, m_symbol is the underlying part
    contract_m_exchange = db.Column(db.String(25), nullable=True)
    contract_m_strike = db.Column(db.Numeric(15, 6), nullable=True)

    ibcontract_conid = db.Column(db.BigInteger, db.ForeignKey('ibcontract.conid'))

    def __repr__(self):
        return f'<Ibexecutionrestful:: execId: {self.execution_m_execId} for asset: {self.ibcontract_conid}, execQty: {self.execution_m_shares} @ {self.execution_m_price}>'


class Ibsymbology(db.Model):
    ticker = db.Column(db.String(75))
    bbgIdentifier = db.Column(db.String(50), nullable=True)
    bbgUnderylingId = db.Column(db.String(50), nullable=True)
    internalUnderlying = db.Column(db.String(50), nullable=True)
    created = db.Column(db.DateTime, server_default=db.func.now())
    updated = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now())


    ibcontract_conid = db.Column(db.BigInteger, db.ForeignKey('ibcontract.conid'), primary_key=True)
    ibcontract = db.relationship("Ibcontract", back_populates="bloom")

    def __repr__(self):
        return f'<Ibsymbology:: {self.ibcontract_conid} - {self.ticker}>'

