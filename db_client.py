import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from models import Account, ProductAttributes
from utils import write_event_log


class DbClient():
    def __init__(self, db_type, db_name, host, port, user, password):
        self.db = (f'{db_type}://{user}:{password}'
                   f'@{host}:{port}/{db_name}')
        try:
            self.engine = sq.create_engine(self.db)
        except sq.exc.NoSuchModuleError as error:
            write_event_log(error, 'DbClient.__init__')
            raise error

    def start_session(self):
        try:
            Session = sessionmaker(bind=self.engine)
            return Session()
        except sq.exc.OperationalError as error:
            write_event_log(error, 'DbClient.start_session')
            raise error

    def add_record(self, db_session, model, **kwargs):
        db_session.add(model(**kwargs))
        return db_session

    def get_credentials(self, mp_id)->list:
        credentials = []
        db_session = self.start_session()
        response = db_session.query(
            Account.client_id_api,
            Account.api_key,
        ).filter(Account.mp_id == mp_id).all()
        for _item in response:
            credentials.append({
                'client_id': _item[0],
                'api_key': _item[1],
            })
        return credentials

    def get_product_attributes(self, product_id:str):
        product_info = []
        db_session = self.start_session()
        response = db_session.query(
            ProductAttributes.attribute_id,
            ProductAttributes.value,
            ProductAttributes.dictionary_value_id,
            ProductAttributes.complex_id,
        ).filter(ProductAttributes.product_id == product_id).all()
        for _item in response:
            product_info.append({
                'attribute_id': _item[0],
                'value': _item[1],
                'dictionary_value_id': _item[2],
                'complex_id': _item[3],
            })
        return product_info

    def remove_duplicates(self, table, partition):
        connection = self.engine.connect()
        connection.execute(f"""
            DELETE
            FROM {table}
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id, 
                    row_number() OVER (
                        PARTITION BY {partition}
                        ORDER BY id DESC
                    )
                    FROM {table}) as query
                WHERE row_number != 1
            );
        """)
