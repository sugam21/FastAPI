import time

import mysql.connector
from loguru import logger


def connect_to_mysql(config, attempts=3, delay=2):
    attempt = 1
    # Implement a reconnection routine
    while attempt < attempts + 1:
        try:
            result = mysql.connector.connect(**config)
            return result
        except (mysql.connector.Error, IOError) as err:
            if attempts is attempt:
                # Attempts to reconnect failed; returning None
                logger.info(f"Failed to connect, exiting without a connection: {err}")
                return None
            logger.info(
                f"Connection failed: {err}. Retrying ({attempt}/{attempts-1})...",
            )
            # progressive reconnect delay
            time.sleep(delay**attempt)
            attempt += 1
    return None
