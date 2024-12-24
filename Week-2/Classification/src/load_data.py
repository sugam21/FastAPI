import os
from abc import ABC, abstractmethod

import mysql.connector
import pandas as pd
from loguru import logger


class DataIngestor(ABC):
    """Abstract class for ingesting data."""

    @abstractmethod
    def ingest(
        self,
        db_name: str | None = None,
        table_name: str | None = None,
        db_password: str | None = None,
        db_host: str | None = None,
    ) -> pd.DataFrame | None:
        """Abstract method to ingest the data."""
        pass


class SQLDataIngestor(DataIngestor):
    """Connects to a SQL database and returns required table as pandas dataframe."""

    @staticmethod
    def connect_database(name, password, host):
        """Creates a connection to mysql database and returns the connection if no error else None."""
        try:
            connection = mysql.connector.connect(
                host=host,
                database=name,
                user="root",
                password=password,
                use_pure=True,
            )
        except (mysql.connector.Error, IOError) as e:
            logger.error(f"Failed to connect, exiting without connection, {str(e)}")
            return None
        else:
            logger.info("Database connected successfully.")
            return connection

    def ingest(
        self,
        db_name: str | None = None,
        table_name: str | None = None,
        db_password: str | None = None,
        db_host: str | None = None,
    ) -> pd.DataFrame | None:

        connection = self.connect_database(
            name=db_name, password=db_password, host=db_host
        )
        if connection and connection.is_connected():
            try:
                # Caution when table name is not correct.
                query: str = f"SELECT * FROM {table_name};"
                diabetes_df: pd.DataFrame = pd.read_sql(query, con=connection)
            except Exception as e:
                logger.error(
                    f"{str(e)}. Make sure your table name or database connection string are correct"
                )
            else:
                return diabetes_df
            finally:
                # Do not forget to close the connection.
                connection.close()

        return None


class DataIngestorFactory:
    @staticmethod
    def get_data_ingestor(file_extension: str = "db") -> DataIngestor | None:
        """Returns approprate data ingestor provided file extension."""

        if file_extension not in ["db", ".csv", ".zip"]:
            logger.error("File extension not supported.")
            return None

        match file_extension:
            case "db":
                return SQLDataIngestor()


if __name__ == "__main__":
    data_ingestor = DataIngestorFactory().get_data_ingestor(file_extension="db")
    table_name = "diabetes"
    if data_ingestor:
        df: pd.DataFrame | None = data_ingestor.ingest(
            db_name=os.getenv("MYSQL_DATABASE"),
            table_name=table_name,
            db_password=os.getenv("MYSQL_PASSWORD"),
            db_host="localhost",
        )
        if df is not None:
            print(df.head())
