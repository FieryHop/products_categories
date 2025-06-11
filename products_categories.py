from pyspark.sql import DataFrame


def get_product_category_pairs(
        products: DataFrame,
        categories: DataFrame,
        relations: DataFrame,
        product_id_col: str = "product_id",
        product_name_col: str = "product_name",
        category_id_col: str = "category_id",
        category_name_col: str = "category_name"
) -> DataFrame:
    products_with_relations = products.join(
        relations,
        products[product_id_col] == relations[product_id_col],
        "left"
    )

    result = products_with_relations.join(
        categories,
        relations[category_id_col] == categories[category_id_col],
        "left"
    )

    return result.select(
        products[product_name_col].alias("product_name"),
        categories[category_name_col].alias("category_name")
    )