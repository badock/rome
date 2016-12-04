from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

eng = create_engine('sqlite:///:memory:')

Base = declarative_base()

NB_ADDRESS = 5


class NetworkAddress(Base):
    __tablename__ = "ip_address"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    allocated = Column(Boolean)
    network_id = Column(Integer)


Base.metadata.bind = eng


def drop_tables():
    try:
        NetworkAddress.__table__.drop()
    except:
        print("Table Book already dropped")
        pass


def create_tables():
    Base.metadata.create_all()


Base.metadata.create_all()

Session = sessionmaker(bind=eng)
ses = Session()


def init_objects():

    next_network_address_id = 1
    for i in range(0, NB_ADDRESS):
        session = get_session()
        network_address_id = next_network_address_id
        network_address = NetworkAddress()
        network_address.id = network_address_id
        network_address.name = '127.0.0.%s' % (network_address_id)
        network_address.allocated = False
        network_address.network_id = None
        session.add_all([network_address])
        next_network_address_id += 1
        session.commit()


def get_session(*args, **kwargs):
    session = Session()
    return session


def get_query(*args, **kwargs):
    return ses.query(*args, **kwargs)
