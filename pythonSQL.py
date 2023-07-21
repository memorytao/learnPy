import sqlalchemy as db
import pandas as pd

tables = {
    "Album": 1,
    "Artist": 1,
    "Customer": 1,
    "Employee": 1,
    "Genre": 1,
    "Invoice": 1,
    "InvoiceLine": 1,
    "MediaType": 1,
    "Playlist": 1,
    "PlaylistTrack": 1,
    "Track": 1,
}

# Create a connection to the database
driver_url = "sqlite:///C:/Users/Tao/AppData/Roaming/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
engine = db.create_engine(driver_url)
sql = 'SELECT * FROM {}'
# Query the data
with engine.connect() as conn:
    for table in tables:
        query = db.text(sql.format(table))
        result = conn.execute(query)

        # Create a Pandas DataFrame from the results of the SQL query
        df = pd.DataFrame(result)

        # Print the first names of all the customers
        print(df.to_string(index=False))
