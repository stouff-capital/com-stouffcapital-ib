"""init table

Revision ID: 57dbc829856f
Revises: 
Create Date: 2018-06-19 08:16:36.549556

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '57dbc829856f'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('contract',
    sa.Column('secType', sa.String(length=25), nullable=True),
    sa.Column('localSymbol', sa.String(length=75), nullable=False),
    sa.Column('symbol', sa.String(length=50), nullable=True),
    sa.Column('currency', sa.String(length=5), nullable=True),
    sa.Column('exchange', sa.String(length=25), nullable=True),
    sa.Column('primaryExchange', sa.String(length=25), nullable=True),
    sa.Column('lastTradeDateOrContractMonth', sa.String(length=15), nullable=True),
    sa.Column('multiplier', sa.Numeric(precision=15, scale=6), nullable=True),
    sa.Column('strike', sa.Numeric(precision=15, scale=6), nullable=True),
    sa.Column('right', sa.String(length=25), nullable=True),
    sa.PrimaryKeyConstraint('localSymbol')
    )
    op.create_table('bbg',
    sa.Column('ticker', sa.String(length=75), nullable=True),
    sa.Column('bbgIdentifier', sa.String(length=50), nullable=True),
    sa.Column('bbgUnderylingId', sa.String(length=50), nullable=True),
    sa.Column('internalUnderlying', sa.String(length=50), nullable=True),
    sa.Column('contract_localSymbol', sa.String(length=75), nullable=False),
    sa.ForeignKeyConstraint(['contract_localSymbol'], ['contract.localSymbol'], ),
    sa.PrimaryKeyConstraint('contract_localSymbol')
    )
    op.create_table('execution',
    sa.Column('execId', sa.String(length=140), nullable=False),
    sa.Column('orderId', sa.BigInteger(), nullable=True),
    sa.Column('contract_localSymbol', sa.String(length=75), nullable=True),
    sa.Column('time', sa.DateTime(), nullable=True),
    sa.Column('acctNumber', sa.String(length=25), nullable=True),
    sa.Column('exchange', sa.String(length=25), nullable=True),
    sa.Column('side', sa.String(length=15), nullable=True),
    sa.Column('shares', sa.BigInteger(), nullable=True),
    sa.Column('cumQty', sa.BigInteger(), nullable=True),
    sa.Column('price', sa.Numeric(precision=15, scale=6), nullable=True),
    sa.Column('avgPrice', sa.Numeric(precision=15, scale=6), nullable=True),
    sa.Column('permId', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['contract_localSymbol'], ['contract.localSymbol'], ),
    sa.PrimaryKeyConstraint('execId')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('execution')
    op.drop_table('bbg')
    op.drop_table('contract')
    # ### end Alembic commands ###
