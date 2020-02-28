import os

# Install Oracle Instant Client following these instructions:
# https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html#ic_x64_inst
# Also install cx_Oracle from PyPI

# Assume the user has stored user/password in environment variables.
USERNAME = os.environ["CPAS_USERNAME"]
PASSWORD = os.environ["CPAS_PASSWORD"]
HOSTNAME = "10.32.196.224"
PORT = "1332"
SERVICE_NAME = "visispd1.lacity.org"

# Connecting to CPAS with cx_Oracle
import cx_Oracle  # noqa: E402

dsn = cx_Oracle.makedsn(HOSTNAME, PORT, service_name=SERVICE_NAME)
connection = cx_Oracle.connect(user=USERNAME, password=PASSWORD, dsn=dsn)

# Connecting to CPAS with SQLAlchemy
import sqlalchemy  # noqa: E402

engine = sqlalchemy.create_engine(
    f"oracle+cx_oracle://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/"
    f"?service_name={SERVICE_NAME}"
)

# List users (schemas)
engine.execute(
    "SELECT username as schema_name FROM sys.all_users ORDER BY username"
).fetchall()

# List tables for user (schema)
USER = "GRNT45"
engine.execute(
    f"SELECT table_name FROM all_tables WHERE owner='{USER}'"
).fetchall()
