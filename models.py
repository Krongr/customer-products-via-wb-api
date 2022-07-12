from sqlalchemy import Column, Integer, Text, String, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Marketplace(Base):
    __tablename__ = 'marketplaces_list'
    id = Column(Integer, primary_key=True)
    mp_name = Column(String, nullable=False)
    description = Column(Text)

class Account(Base):
    __tablename__ = 'account_list'
    id = Column(Integer, primary_key=True, autoincrement=True)
    mp_id = Column(Integer, ForeignKey(Marketplace.id))
    client_id_api = Column(String)
    api_key = Column(Text)

class ProductAttributes(Base):
    __tablename__ = 'product_attr'
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String)
    attribute_id = Column(String)
    value = Column(Text)    
    dictionary_value_id = Column(String)
    complex_id = Column(String)
    mp_id = Column(Integer, ForeignKey(Marketplace.id))
    db_i = Column(String)  # Index: combined product ID and attribute ID value
