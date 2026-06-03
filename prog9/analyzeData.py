from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType,
    IntegerType, StringType,
    StructField, StructType
)


def printRanked(df, title, order_col):
    print(f"\n{title}")
    df.orderBy(F.col(order_col).desc()).show(
        100,
        truncate=False,
    )


def main():
    spark = (
        SparkSession.builder
        .appName("TCG dataset")
        .getOrCreate()
    )

    gameStatsSchema = StructType([
        StructField("category_id", IntegerType(), True),
        StructField("game", StringType(), True),
        StructField("total_products", IntegerType(), True),
        StructField("with_pricing", IntegerType(), True),
        StructField("avg_market_price", DoubleType(), True),
        StructField("snapshot_date", DateType(), True),
    ])

    marketDataSchema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("clean_name", StringType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("market_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("mid_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("price_date", DateType(), True),
        StructField("drift", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
    ])

    dfGameStats = spark.read.csv(
        "./dataset/tcg_game_stats.csv",
        header=True,
        schema=gameStatsSchema,
        mode="PERMISSIVE",
    )

    dfMarketData = spark.read.csv(
        "./dataset/tcg_market_data.csv",
        header=True,
        schema=marketDataSchema,
        mode="PERMISSIVE",
    )

    nonCardPattern = (
        "booster|box|case|display|pack|deck|starter|bundle|"
        "sealed|collection|tin|sleeve|playmat|binder|dice|"
        "portfolio|album|protector|kit|set of|bulk"
    )

    individualCards = dfMarketData.where(
        ~F.lower(F.col("name")).rlike(nonCardPattern)
    )

    avgMarketByCategory = (
        individualCards
        .groupBy("category_id")
        .agg(
            F.avg("market_price").alias("avg_card_price"),
            F.avg("drift").alias("avg_drift"),
            F.avg("volatility").alias("avg_volatility"),
        )
    )

    tcgRankings = (
        dfGameStats
        .where(~F.col("game").startswith("Category "))
        .select("category_id", "game", "total_products")
        .join(avgMarketByCategory, on="category_id", how="left")
        .select(
            "game",
            "total_products",
            F.round("avg_card_price", 2).alias("avg_card_price"),
            F.round("avg_drift", 6).alias("avg_drift"),
            F.round("avg_volatility", 6).alias("avg_volatility"),
        )
    )

    printRanked(
        tcgRankings,
        "TCGs ranked by average card price",
        "avg_card_price"
    )
    printRanked(
        tcgRankings,
        "TCGs ranked by total products",
        "total_products"
    )
    printRanked(
        tcgRankings,
        "TCGs ranked by average drift",
        "avg_drift"
    )
    printRanked(
        tcgRankings,
        "TCGs ranked by average volatility",
        "avg_volatility"
    )

    spark.stop()


if __name__ == "__main__":
    main()
