import dlt

rules = {
    "rule_1": "product_id is not null",
    "rule_2": "price > 0"
}
@dlt.table()
@dlt.expect_all_or_drop(rules)
def products_stg():
    df = spark.readStream.table("dltvamsi.source.products")
    return df