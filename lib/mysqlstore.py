# Consolidation for mysql related functions
# Some information at
# https://stackoverflow.com/questions/24475645/sqlalchemy-one-to-one-relation-primary-as-foreign-key

# import logging
import os
import pymysql


class DirectConn:
    """
    This class will set up a direct connection to MySQL.
    For Drupal lkb this seems a better idea than emulating the database in a SQLAlchemy way.
    """

    def __init__(self):
        """
        The init procedure will set-up Connection to the Database Server, but not to a specific database.
        """

        mysql_conn = dict(
            host=os.getenv('MYSQL_HOST') or 'localhost',
            port=os.getenv('MYSQL_PORT') or 3306,
            user=os.getenv('MYSQL_USER'),
            passwd=os.getenv('MYSQL_PWD'),
            db=os.getenv('MYSQL_DB')
        )
        self.conn = pymysql.connect(**mysql_conn)
        self.cur = self.conn.cursor(pymysql.cursors.DictCursor)

    def close(self):
        self.conn.close()

    def get_cds(self):
        """
        This method will collect cd information and return a cursor with CD information

        :return: Cursor with CD information.
        """
        query = """
            SELECT 
                nid, title, created, changed
            FROM
                node
            WHERE
                type = 'cd'
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_dirigentnamen(self):
        """
        This method will collect Dirigent namen and return a dictionary with key Kompositie ID and value
        Dirigent Naam.

        :return: Dictionary with key Kompositie ID and value Dirigent Naam.
        """
        query = """
            SELECT 
                entity_id, field_dirigent_naam_value
            FROM
                field_data_field_dirigent_naam
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        dirigent_dict = {}
        for rec in res:
            dirigent_dict[rec['entity_id']] = rec['field_dirigent_naam_value']
        return dirigent_dict

    def get_dirigentvoornamen(self):
        """
        This method will collect Dirigent voornamen and return a dictionary with key Kompositie ID and value
        Dirigent Naam.

        :return: Dictionary with key Kompositie ID and value Dirigent Naam.
        """
        query = """
            SELECT 
                entity_id, field_dirigent_voornaam_value
            FROM
                field_data_field_dirigent_voornaam
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        dirigent_dict = {}
        for rec in res:
            dirigent_dict[rec['entity_id']] = rec['field_dirigent_voornaam_value']
        return dirigent_dict

    def get_identificaties(self):
        """
        This method will collect identificatie information and return a dictionary with key CD ID and value
        Identificatie.

        :return: Dictionary with key CD ID and value Identificatie.
        """
        query = """
            select entity_id, field_identificatie_value 
            from field_data_field_identificatie 
            where not field_identificatie_value = 'NULL'
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        identificatie_dict = {}
        for rec in res:
            identificatie_dict[rec['entity_id']] = rec['field_identificatie_value']
        return identificatie_dict

    def get_kompositie_to_cd(self):
        """
        This method will collect Kompositie to CD information.

        :return: Dictionary with key Kompositie ID and value CD ID.
        """
        query = """
            SELECT 
                entity_id, field_cd_target_id
            FROM
                field_data_field_cd
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        kompositie_to_cd_dict = {}
        for rec in res:
            kompositie_to_cd_dict[rec['entity_id']] = rec['field_cd_target_id']
        return kompositie_to_cd_dict

    def get_kompositie_to_komponist(self):
        """
        This method will collect Kompositie to Komponist information.

        :return: Dictionary with key Kompositie ID and value CD ID.
        """
        query = """
            SELECT 
                entity_id, field_komponist_target_id
            FROM
                field_data_field_komponist
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        kompositie_to_cd_dict = {}
        for rec in res:
            kompositie_to_cd_dict[rec['entity_id']] = rec['field_komponist_target_id']
        return kompositie_to_cd_dict

    def get_komponisten(self):
        """
        This method will collect komponisten information and return a cursor with this information

        :return: Cursor with Komponisten information.
        """
        query = """
            SELECT 
                nid, title, created, changed
            FROM
                node
            WHERE
                type = 'komponist'
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_komposities(self):
        """
        This method will collect kompositie information and return a cursor with this information

        :return: Cursor with Kompositie information.
        """
        query = """
            SELECT 
                nid, title, created, changed
            FROM
                node
            WHERE
                type = 'kompositie'
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_uitgevers(self):
        """
        This method will collect uitgever information and return a dictionary with key CD ID and value Uitgever..

        :return: Dictionary with key CD ID and value Uitgever.
        """
        query = """
            select entity_id, field_uitgever_value 
            from field_data_field_uitgever 
            where not field_uitgever_value = 'NULL'
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        uitgever_dict = {}
        for rec in res:
            uitgever_dict[rec['entity_id']] = rec['field_uitgever_value']
        return uitgever_dict

    def get_uitvoerders(self):
        """
        This method will collect volgnummer uitvoerders and return a dictionary with key Kompositie ID and value
        Uitvoerders.

        :return: Dictionary with key Kompositie ID and value Uitvoerders.
        """
        query = """
            SELECT 
                entity_id, field_uitvoerders_value
            FROM
                field_data_field_uitvoerders
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        uitvoerders_dict = {}
        for rec in res:
            uitvoerders_dict[rec['entity_id']] = rec['field_uitvoerders_value']
        return uitvoerders_dict

    def get_voornamen(self):
        """
        This method will collect Komponist voornamen and return a dictionary with key Komponist ID and value
        Voornaam.

        :return: Dictionary with key Komponist ID and value Voornaam.
        """
        query = """
            SELECT 
                entity_id, field_voornaam_value
            FROM
                field_data_field_voornaam
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        dirigent_dict = {}
        for rec in res:
            dirigent_dict[rec['entity_id']] = rec['field_voornaam_value']
        return dirigent_dict

    def get_volgnummers(self):
        """
        This method will collect volgnummer information and return a dictionary with key Kompositie ID and value
        Volgnummer.

        :return: Dictionary with key Kompositie ID and value Volgnummer.
        """
        query = """
            SELECT 
                entity_id, field_volgnummer_value
            FROM
                field_data_field_volgnummer
            """
        self.cur.execute(query)
        res = self.cur.fetchall()
        volgnummer_dict = {}
        for rec in res:
            volgnummer_dict[rec['entity_id']] = rec['field_volgnummer_value']
        return volgnummer_dict
