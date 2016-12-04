from rome.core.session.session import Session
from sqlalchemy_models import NetworkAddress, NB_ADDRESS


def drop_tables():
    drop_table_session = Session()
    for obj in get_query(NetworkAddress).all():
        drop_table_session.delete(obj)
    drop_table_session.commit()


def create_tables():
    # Base.metadata.create_all()
    pass


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
    from rome.core.session.session import Session as RomeSession
    session = RomeSession()
    return session


def get_query(*args, **kwargs):
    from rome.core.session.session import Session as RomeSession
    session = RomeSession()
    query = session.query(*args, **kwargs)

    return query
