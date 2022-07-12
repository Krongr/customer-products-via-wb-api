import requests
import sqlalchemy
from db_client import DbClient
from models import ProductAttributes
from wb_api import WbApi
from utils import write_event_log


# DB settings:
TYPE= 'postgresql'
NAME= ''
HOST= ''
PORT= ''
USER= ''
PASSWORD= ''


def collect_products_attributes(wb:WbApi, offset=0, 
                                products_with_attributes=None):
    """Returns a list of attributes of the client's products.
    """
    products_with_attributes = products_with_attributes or []
    try:
        response = wb.product_cards(offset)
    except requests.exceptions.ConnectionError as error:
        write_event_log(error, 'wb.product_cards')
        return products_with_attributes

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        write_event_log(error, 'collect_products_attributes', response.json())
        return products_with_attributes

    try:
        product_cards = response.json()['result']['cards']
    except KeyError as error:
        write_event_log(error, 'collect_products_attributes', response.json())
        return products_with_attributes

    if product_cards:
        try:
            products_with_attributes += product_cards
            collect_products_attributes(
                wb,
                offset+1000,
                products_with_attributes
            )
        except TypeError as error:
            write_event_log(error, 'collect_products_attributes')
    return products_with_attributes

def add_product_attribute_records(db:DbClient, product_card,
                                               db_session):
    """Returns a DB session with created product attributes records.
    """
    try:
        imt_attributes = {
            'imtId': product_card['imtId'],
            'supplierId': product_card['supplierId'],
            'object': product_card['object'],
            'parent': product_card['parent'],
            'countryProduction': product_card['countryProduction'],
        }

        for _attribute in product_card['addin']:
            for _parameter in _attribute['params']:
                imt_attributes[_attribute['type']] = (
                    _parameter.get('value') or _parameter.get('count')
                )

        for _entry in product_card['nomenclatures']:
            nm_attributes = {
                'vendorCode': _entry['vendorCode'],
                'barcodes': '|'.join(_entry['variations'][0]['barcodes'])
            }
            for _id, _value in nm_attributes.items():
                db_session = db.add_record(
                            db_session=db_session,
                            model=ProductAttributes,
                            attribute_id=_id,
                            value=_value,
                            product_id=_entry['nmId'],
                            db_i=f"{_entry['nmId']}{_id}",
                            mp_id=2,
                )

            for _id, _value in imt_attributes.items():
                db_session = db.add_record(
                            db_session=db_session,
                            model=ProductAttributes,
                            attribute_id=_id,
                            value=_value,
                            product_id=_entry['nmId'],
                            db_i=f"{_entry['nmId']}{_id}",
                            mp_id=2,
                )

            for _attribute in _entry['addin']:
                for _parameter in _attribute['params']:
                    db_session = db.add_record(
                        db_session=db_session,
                        model=ProductAttributes,
                        attribute_id=_attribute['type'],
                        value=(_parameter.get('value') or 
                               _parameter.get('count')),
                        product_id=_entry['nmId'],
                        db_i=f"{_entry['nmId']}{_attribute['type']}",
                        mp_id=2,
                    )

    except (KeyError, TypeError) as error:
        write_event_log(error, 'add_product_category_and_attribute_records')

    return db_session


if __name__ == '__main__':
    db = DbClient(TYPE, NAME, HOST, PORT, USER, PASSWORD)

    try:
        credentials = db.get_credentials(mp_id=2)
    except (
        sqlalchemy.exc.OperationalError,
        sqlalchemy.exc.InternalError,
        sqlalchemy.exc.ProgrammingError,
    ) as error:
        write_event_log(error, 'DbClient.get_credentials')
        raise error

    for _entry in credentials:
        wb = WbApi(_entry['api_key'])

        # Collect the attributes of the client's products:
        products_with_attributes = collect_products_attributes(wb)
        try:
            assert products_with_attributes
        except AssertionError:
            write_event_log(
                f"'products_with_attributes' is empty",
                'collect_products_attributes',
            )
            continue

        # Record attributes of the client's products:
        category_ids = set()
        db_session = db.start_session()
        for _product_card in products_with_attributes:
            db_session = add_product_attribute_records(
                db,
                _product_card,
                db_session,
            )

        try:
            db_session.commit()
        except (
            sqlalchemy.exc.InternalError,
            sqlalchemy.exc.IntegrityError,
            sqlalchemy.exc.ProgrammingError,
            sqlalchemy.exc.DataError,
            sqlalchemy.exc.OperationalError,
        ) as error:
            write_event_log(error, 'products_with_attributes.commit')

        #Remove duplicates:
        try:
            db.remove_duplicates(ProductAttributes.__tablename__, 'db_i')
        except (
            sqlalchemy.exc.InternalError,
            sqlalchemy.exc.IntegrityError,
            sqlalchemy.exc.ProgrammingError,
            sqlalchemy.exc.DataError,
            sqlalchemy.exc.OperationalError,
        ) as error:
            write_event_log(
                error,
                'add_product_attribute_records db.remove_duplicates',
            )
