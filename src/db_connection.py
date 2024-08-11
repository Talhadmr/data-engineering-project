from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker as sessionMaker
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
from dotenv import load_dotenv
import os
from abc import ABC, abstractmethod

# Load the .env file
load_dotenv()

class BaseDBConnection(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def connect(self):
        pass

class PostgreSQLDB(BaseDBConnection):

    def __init__(self):

        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT")
        self.db = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")

        # Initialize SQLAlchemy Engine and Session objects
        self.engine: Engine = None
        self.Session: Session = None

    def connect(self):
        try:
            self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
            self.Session = sessionMaker(bind=self.engine)
            print("Successfully connected to: ", self.engine.url)
        except Exception as e:
            print("Connection failed: ", e)
        return self.engine, self.Session

    def disconnect(self):

        try:
            if self.engine:
                self.engine.dispose()
                print("Disconnected from: ", self.engine.url)
            else:
                print("Already disconnected.")    
        except Exception as e:
            print("Disconnection failed: ", e)

    def execute_query(self, query):
        try: 
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                print("Successfully executed.")
                connection.commit()
                return result
        except Exception as e:
            print("Failed: ", e)
