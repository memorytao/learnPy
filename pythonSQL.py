import sqlalchemy as db
import pandas as pd

# Create a connection to the database
driver_url = "sqlite:///C:/Users/Tao/AppData/Roaming/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
engine = db.create_engine(driver_url)

# Query the data
with engine.connect() as conn:
    query = db.text('SELECT * FROM Customer')
    result = conn.execute(query)

    # Create a Pandas DataFrame from the results of the SQL query
    df = pd.DataFrame(result)

    # Print the first names of all the customers
    print(df['FirstName'].to_string(index=False))
