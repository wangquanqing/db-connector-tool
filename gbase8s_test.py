import os

from sqlalchemy import create_engine, text


engine = create_engine('jdbcapi+mysql://cvicse:Cvicsejszx%402022@localhost:3306/db_station')

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM users"))
    for row in result:
        print(row)
