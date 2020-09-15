from werkzeug.local import LocalProxy
import dataset
import os.path


_postgres_db = None


def get_postgres_database(app, schema=None):
    global _postgres_db
    if _postgres_db is None:
        connection_str = (
            f"postgres+psycopg2://{app.config['DB_USER']}:{app.config['DB_PASSWORD']}"
            + f"@{app.config['DB_HOST']}/{app.config['DB_NAME']}"
        )

        print(connection_str)
        _postgres_db = dataset.connect(
            connection_str
        )
    return _postgres_db


postgres_db = LocalProxy(get_postgres_database)
