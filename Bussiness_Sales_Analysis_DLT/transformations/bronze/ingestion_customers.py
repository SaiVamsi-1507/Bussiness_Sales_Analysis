import dlt

rules = {
    "rule_1": "customer_id IS NOT NULL",
}
@dlt.table()
@dlt.expect_all(rules)
def customers_stg():
    df = spark.readStream.table("dltvamsi.source.customers")
    return df