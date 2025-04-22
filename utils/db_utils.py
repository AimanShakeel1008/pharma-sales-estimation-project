import psycopg2 as postgresql
import configparser
from datetime import datetime


config = configparser.ConfigParser()
config.read('config/settings.ini')

DB_HOST = config['db']['host']
DB_USER = config['db']['user']
DB_PASS = config['db']['password']
DB_NAME = config['db']['database']
DB_PORT = config['db']['port']

# Column Mappings for each table
column_mappings = {
    "regions": {
        "RegionID": "region_id",
        "RegionName": "region_name",
        "IsActive": "is_active"
    },
    "countries": {
        "CountryID": "country_id",
        "CountryName": "country_name",
        "RegionID": "region_id",
        "IsActive": "is_active"
    },
    "companies": {
        "CompanyID": "company_id",
        "CompanyName": "company_name",
        "IsActive": "is_active"
    },
    "categories": {
        "CategoryID": "category_id",
        "CategoryName": "category_name",
        "IsActive": "is_active"
    },
    "drugs": {
        "DrugID": "drug_id",
        "DrugName": "drug_name",
        "CategoryID": "category_id",
        "CompanyID": "company_id",
        "ActiveIngredient": "active_ingredient",
        "DosageForm": "dosage_form",
        "Strength": "strength",
        "ApprovalStatus": "approval_status",
        "MarketLaunchYear": "market_launch_year",
        "SideEffects": "side_effects",
        "PrescriptionRequired": "prescription_required",
        "IsActive": "is_active"
    },
    "drug_rankings": {
        "CountryID": "country_id",
        "Quarter": "quarter",
        "CategoryID": "category_id",
        "DrugID": "drug_id",
        "Rank": "rank"
    },
    "category_sales": {
        "CountryID": "country_id",
        "Quarter": "quarter",
        "CategoryID": "category_id",
        "TotalUnitsSold": "total_units_sold",
        "TotalRevenue": "total_revenue"
    },
    "sales_our_company": {
        "DrugID": "drug_id",
        "CountryID": "country_id",
        "Quarter": "quarter",
        "UnitsSold": "units_sold",
        "Revenue": "revenue"
    }
}


def save_input_data_to_db(input_data):
    global conn, cursor
    try:
        conn = postgresql.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        for table_name, df in input_data.items():
            print(f"📦 Inserting data into: {table_name} ...............")
            cursor = conn.cursor()
            mapping = column_mappings[table_name]
            df = df.rename(columns=mapping)

            for _, row in df.iterrows():
                columns = list(row.index)
                values = [row[col] for col in columns]

                # Define unique keys
                if table_name == "regions":
                    keys = ["region_id"]
                elif table_name == "countries":
                    keys = ["country_id"]
                elif table_name == "companies":
                    keys = ["company_id"]
                elif table_name == "categories":
                    keys = ["category_id"]
                elif table_name == "drugs":
                    keys = ["drug_id"]
                elif table_name == "drug_rankings":
                    keys = ["country_id", "quarter", "drug_id"]
                elif table_name == "category_sales":
                    keys = ["country_id", "quarter", "category_id"]
                elif table_name == "sales_our_company":
                    keys = ["drug_id", "country_id", "quarter"]
                else:
                    raise Exception("Unknown table")

                placeholders = ', '.join(['%s'] * len(columns))
                update_stmt = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in keys])
                insert_stmt = f"""
                        INSERT INTO {table_name} ({', '.join(columns)})
                        VALUES ({placeholders})
                        ON CONFLICT ({', '.join(keys)}) DO UPDATE SET
                        {update_stmt}
                    """
                cursor.execute(insert_stmt, values)
            conn.commit()
            print(f"✅ Inserted data into: {table_name} successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def save_estimated_data_to_rds(df, table_name):
    """Save DataFrame to RDS"""
    global conn, cursor
    try:
        conn = postgresql.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        cursor = conn.cursor()

        # Define the column names (excluding 'id')
        columns = [
            "country_id", "country_name", "quarter", "category_id", "category_name",
            "drug_id", "drug_name", "company_id", "company_name", "rank",
            "mean_sales", "min_sales", "max_sales"
        ]

        # Create a comma-separated string of column names
        column_str = ', '.join(columns)
        # print("column_str:", column_str)

        # Insert each row, excluding 'id'
        for _, row in df.iterrows():
            values = tuple(row[col] for col in columns)  # only values for specified columns
            sql = f"INSERT INTO {table_name} ({column_str}) VALUES {values}"
            cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def save_country_sales_to_rds(df, table_name):
    """Save DataFrame to RDS"""
    global conn, cursor
    try:
        conn = postgresql.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        cursor = conn.cursor()

        # Define the column names (excluding 'id')
        columns = [
            "country_id", "country_name", "quarter", "total_revenue"
        ]

        # Create a comma-separated string of column names
        column_str = ', '.join(columns)

        # Insert each row, excluding 'id'
        for _, row in df.iterrows():
            values = tuple(row[col] for col in columns)  # only values for specified columns
            # print("values:",values)
            sql = f"INSERT INTO {table_name} ({column_str}) VALUES {values}"
            cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def save_country_category_sales_to_rds(df, table_name):
    """Save DataFrame to RDS"""
    global conn, cursor
    try:
        conn = postgresql.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        cursor = conn.cursor()

        # Define the column names (excluding 'id')
        columns = [
            "country_id", "country_name", "quarter", "category_id", "category_name", "total_revenue"
        ]

        # Create a comma-separated string of column names
        column_str = ', '.join(columns)
        # print("column_str:", column_str)

        # Insert each row, excluding 'id'
        for _, row in df.iterrows():
            values = tuple(row[col] for col in columns)  # only values for specified columns
            # print("values:",values)
            sql = f"INSERT INTO {table_name} ({column_str}) VALUES {values}"
            cursor.execute(sql)

        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_status(quarter, status, message=None, completed=False):
    global conn, cursor
    try:
        conn = postgresql.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        cursor = conn.cursor()

        if status == 'In Progress':
            # Insert a new status row
            cursor.execute("""
                INSERT INTO estimation_status (quarter, status, message)
                VALUES (%s, %s, %s)
            """, (quarter, status, message))
        else:
            # Update the latest record for this quarter
            cursor.execute("""
                UPDATE estimation_status
                SET status = %s,
                    message = %s,
                    completed_at = %s
                WHERE id = (
                    SELECT id FROM estimation_status
                    WHERE quarter = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                )
            """, (status, message, datetime.now(), quarter))
        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
