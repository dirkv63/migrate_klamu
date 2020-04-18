"""
This module consolidates sqlite Database functions for the klamu project.
"""

import logging
import os
import sqlite3
from sqlalchemy import Column, Integer, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Cd(Base):
    """
    Table wit CD information.
    """
    __tablename__ = "cd"
    id = Column(Integer, primary_key=True)
    created = Column(Integer, nullable=False)
    modified = Column(Integer, nullable=False)
    identificatie = Column(Text)
    titel = Column(Text)
    uitgever = Column(Text)


class Kompositie(Base):
    """
    Table wit Kompositie information.
    """
    __tablename__ = "kompositie"
    id = Column(Integer, primary_key=True)
    created = Column(Integer, nullable=False)
    modified = Column(Integer, nullable=False)
    dirigent_naam = Column(Text)
    dirigent_voornaam = Column(Text)
    naam = Column(Text, nullable=False)
    uitvoerders = Column(Text)
    volgnummer = Column(Integer)
    cd_id = Column(Integer, nullable=False)
    komponist_id = Column(Integer)


class Komponist(Base):
    """
    Table wit Komponist information.
    """
    __tablename__ = "komponist"
    id = Column(Integer, primary_key=True)
    created = Column(Integer, nullable=False)
    modified = Column(Integer, nullable=False)
    naam = Column(Text, nullable=False)
    voornaam = Column(Text, nullable=False)


class DirectConn:
    """
    This class will set up a direct connection to the database. It allows to reset the database,
    in which case the database will be dropped and recreated, including all tables.
    """

    def __init__(self, db):
        """
        To drop a database in sqlite3, you need to delete the file.

        :param db: Name of the Database to connect.
        """
        self.db = db
        self.dbConn = ""
        self.cur = ""

    def _connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        db_conn = sqlite3.connect(self.db)
        # db_conn.row_factory = sqlite3.Row
        logging.debug("Datastore object and cursor are created")
        return db_conn, db_conn.cursor()

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        os.remove(self.db)
        # Reconnect to the Database
        self.dbConn, self.cur = self._connect2db()
        # Use SQLAlchemy connection to build the database
        conn_string = "sqlite:///{db}".format(db=self.db)
        engine = set_engine(conn_string=conn_string)
        Base.metadata.create_all(engine)


def init_session(db, echo=False):
    """
    This function configures the connection to the database and returns the session object.

    :param db: Name of the sqlite3 database.
    :param echo: True / False, depending if echo is required. Default: False
    :return: session object.
    """
    conn_string = "sqlite:///{db}".format(db=db)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def set_engine(conn_string, echo=False):
    engine = create_engine(conn_string, echo=echo)
    return engine


def set_session4engine(engine):
    session_class = sessionmaker(bind=engine)
    session = session_class()
    return session
