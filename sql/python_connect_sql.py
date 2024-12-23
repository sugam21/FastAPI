import os

from dotenv import load_dotenv
from loguru import logger
from myconnection import connect_to_mysql

load_dotenv()


def main() -> None:
    config: dict = {
        "host": os.getenv("HOST"),
        "user": os.getenv("MYSQL_USERNAME"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": "bfhl",
    }
    account_id: str = "0012j00000GjoGnAAJ"
    connection = connect_to_mysql(config)
    if connection and connection.is_connected():
        with connection.cursor() as cursor:
            try:
                result = cursor.execute(
                    f"""
                        SELECT 
                            t.AccountId, t.Name, t.Age, t.City, t.State, t.Pincode,
                            t.Id, t.CreatedDate, t.CaseNumber, t.HAN, t.BillAmount, t.Status,
                            p.HAN, p.`Policy Name`
                        FROM 
                        (SELECT 
                            a.AccountId, a.Name, a.Age, a.City, a.State, a.Pincode,
                            c.Id, c.CreatedDate, c.CaseNumber, c.HAN, c.BillAmount, c.Status
                        FROM accounts as a
                        LEFT JOIN claims as c
                        ON a.AccountId = c.AccountId) as t
                        LEFT JOIN bfhl.policies as p
                        ON t.HAN = p.HAN
                        WHERE t.AccountId = "{account_id}"
                        LIMIT 2
                        """
                )
            except Exception as e:
                logger.error(e)
            else:
                rows = cursor.fetchall()
                for rows in rows:
                    print(rows)
                connection.close()
    else:
        print("Could not connect")


if __name__ == "__main__":
    main()
