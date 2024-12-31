from feast import Entity

city = Entity(
    name="city",
    join_keys=["city"],
)
