import pandas as pd
import numpy as np

from utils.db_utils import save_estimated_data_to_rds, save_country_category_sales_to_rds, save_country_sales_to_rds

DATA_FOLDER = "data"


def sales_estimation(data):
    df_category_country_sales = data["category_sales"].merge(data["categories"], on="CategoryID", how="left")
    df_category_country_sales = df_category_country_sales.merge(data["countries"], on="CountryID", how="left")

    df_category_country_sales = df_category_country_sales[
        ["CountryID", "CountryName", "Quarter", "CategoryID", "CategoryName", "TotalRevenue"]]

    df_country_sales = df_category_country_sales.groupby(["CountryID", "CountryName", "Quarter"])[
        "TotalRevenue"].sum().reset_index()

    simulated_category_country_sales = []

    for index, row in df_category_country_sales.iterrows():
        # Store results
        simulated_category_country_sales.append({
            "country_id": row["CountryID"],
            "country_name": row["CountryName"],
            "quarter": row["Quarter"],
            "category_id": row["CategoryID"],
            "category_name": row["CategoryName"],
            "total_revenue": row["TotalRevenue"]
        })

    df_simulated_category_country_sales = pd.DataFrame(simulated_category_country_sales)

    print("📦 Inserting Total category sales per country...............")

    save_country_category_sales_to_rds(df_simulated_category_country_sales, "country_category_revenue_summary")

    print("✅ Inserted Total category sales per country data successfully.")

    simulated_country_sales = []

    for index, row in df_country_sales.iterrows():
        # Store results
        simulated_country_sales.append({
            "country_id": row["CountryID"],
            "country_name": row["CountryName"],
            "quarter": row["Quarter"],
            "total_revenue": row["TotalRevenue"]
        })

    df_simulated_country_sales = pd.DataFrame(simulated_country_sales)

    print("📦 Inserting Total sales per country...............")

    save_country_sales_to_rds(df_simulated_country_sales, "country_revenue_summary")

    print("✅ Inserted Total category sales per country data successfully.")

    # Monte Carlo Simulation Settings
    NUM_SIMULATIONS = 10000  # Number of Monte Carlo iterations
    VARIABILITY = 0.1  # 10% sales variability

    # Generate Sales Estimates with Random Variability
    np.random.seed(42)  # For reproducibility

    # Merge rankings with category sales data
    df_estimation = data["drug_rankings"].merge(data["category_sales"], on=["CountryID", "Quarter", "CategoryID"])

    # Assign Weights Based on Rank (Inverse Ranking)
    df_estimation["Weight"] = 1 / df_estimation["Rank"]

    # Normalize Weights
    df_estimation["Normalized_Weight"] = df_estimation.groupby(["CountryID", "CategoryID"])["Weight"].transform(
        lambda x: x / x.sum())

    # Estimate Sales for Each Drug
    df_estimation["Estimated_Sales"] = df_estimation["Normalized_Weight"] * df_estimation["TotalRevenue"]

    # Merge with Drug & Company Details
    df_estimation = df_estimation.merge(
        data["drugs"][['DrugID', 'DrugName', 'CompanyID', 'CategoryID']],
        on="DrugID",
        how="left"
    )

    # Fix duplicate CategoryID issue
    df_estimation["CategoryID"] = df_estimation["CategoryID_x"].fillna(df_estimation["CategoryID_y"])
    df_estimation.drop(columns=["CategoryID_x", "CategoryID_y"], inplace=True)

    # Merge with Categories to get CategoryName
    df_estimation = df_estimation.merge(data["categories"][['CategoryID', 'CategoryName']], on="CategoryID", how="left")

    # Merge with Companies to get CompanyName
    df_estimation = df_estimation.merge(data["companies"][['CompanyID', 'CompanyName']], on="CompanyID", how="left")

    # Merge with Countries to get CountryName
    df_estimation = df_estimation.merge(data["countries"][['CountryID', 'CountryName']], on="CountryID", how="left")

    simulated_sales = []

    for index, row in df_estimation.iterrows():
        mean_sales = row["Estimated_Sales"]  # Use ranking-based estimate as mean
        std_dev = mean_sales * VARIABILITY  # 10% standard deviation

        # Generate random sales values (Normal Distribution)
        simulated_values = np.random.normal(mean_sales, std_dev, NUM_SIMULATIONS)

        simulated_sales.append({
            "country_id": row["CountryID"],
            "country_name": row["CountryName"],
            "quarter": row["Quarter"],
            "category_id": row["CategoryID"],
            "category_name": row["CategoryName"],
            "drug_id": row["DrugID"],
            "drug_name": row["DrugName"],
            "company_id": row["CompanyID"],
            "company_name": row["CompanyName"],
            "rank": row["Rank"],
            "mean_sales": np.mean(simulated_values),
            "min_sales": np.percentile(simulated_values, 5),  # 5th percentile
            "max_sales": np.percentile(simulated_values, 95)  # 95th percentile
        })

    # Convert to DataFrame
    df_simulated = pd.DataFrame(simulated_sales)

    print("📦 Inserting Estimated sales...............")

    save_estimated_data_to_rds(df_simulated, "estimated_pharma_sales")

    print("✅ Inserted estimated sales data successfully.")

    # Save the results
    # df_simulated.to_csv(f"{DATA_FOLDER}/calculated_estimated_sales.csv", index=False)

    print("✅ Competitor sales estimation completed!")
