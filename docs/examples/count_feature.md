---
layout: default
title: A simple count feature
parent: Examples
permalink: examples/count_feature
---
# Example: a simple count feature
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## A simple count feature

Imagine you’re doing fraud detection and want to detect whether a transaction is fraudulent before that transaction goes through. This means you’ll be doing online prediction, likely with a strict latency requirement.

A transaction might look like the following. Each transaction is defined by a unique identifier `tid`.

```json
{
    "timestamp": "2022-10-05 10:30:05",
    "tid": "11df919988c134d97bbff2678eb68e22",
    "cc_num": 4473593503484549,
    "amount": 47.52,
    "category": "Grocery",
    "city": "San Francisco",
    "country": "US",
}
```
A feature you might want to use is the number of transactions the credit card has been used up to the point of prediction.

---

## Query vs. compute a feature

For example, given the transaction above where the credit card number is X and the timestamp is T, we want to get the number of transactions this credit card has been used up to T.

- Compute: If we have all the transactions in the last 6 months in a table or a dataframe, we might compute this feature as follows:

    ```python
    # tx is a pandas dataframe
    tx[tx["cc_num"] == X][tx["timestamp"] < T]["cc_num"].count()
    ```

    ```sql
    # tx is a SQL table
    SELECT COUNT(*)
    FROM tx
    WHERE cc_num = X AND timestamp < T
    ```

- Query: if this value is already computed and stored in, say, the column

---

## Dual-source: feature using data from both stream and batch sources

### Same data, different sources

In production, however, transactions might not be in just one place. Assuming you’re using a real-time transport, such as Kafka, to log transactions. Since keeping data in streams is expensive, you might want to keep your data in Kafka for a certain amount of time, say 7 days, before moving it to your data warehouse/lakehouse, such as Snowflake. The data is made available in your data warehouse after some delay, say daily. At the prediction time T, the data needed to compute this feature is in 2 different sources:
- T-7 days: in Kafka.
- Up to T-1 day: in Snowflake.

With the current tooling, there’s a division between stream and batch. When creating a feature, you have to specify whether this is a streaming or a batch feature, with corresponding stream/batch source. For example, to create this simple count feature, you’ll have to create 2 features:
1. **Streaming feature**: transaction count of a credit card over the last 7 days
2. **Batch feature**: transaction account for the same credit card up to T-1 day

This means that you’ll have to be aware of where data is at a given point. For example, you’ll need to know Kafka’s retention policy and write-to-warehouse policy to be able to create these features.

### ResponsiveML's dual source data

ResponsiveML decouples data schema from its sources so that you can create feature logic on the data schema, without having to worry about where this data comes from. If this sounds confusing, don’t worry, we’ll go through examples to explain this.

#### Schemas
You can think of a schema as a data skeleton. It defines the form of the data. For example, we might expect all transactions to follow this schema:

```python
@schema
class TransactionSchema:
    "timestamp": datetime
    "tid": str
    "cc_num": int
    "amount": float
    "category": str
    "city": str
    "country": str
```

#### Sources and DataAssets

Schemas describe the shape of data. We need to fill (hydrate) them with data from sources: stream sources, batch sources, or even CSV files. A DataAsset is a Schema filled with data from specified sources.

For example, transactions is a DataAsset that might start at Kafka and ends up later in a warehouse like Snowflake.

1. [Recommended] If you just connect the schema to Kafka, we’ll automatically back up your data streams with a batch source.

    ```python
    transaction_asset = fw.DataAsset(
        schema=TransactionSchema,
        stream_source=KafkaSource(...),
    )
    ```

2. If you already have your data in Snowflake and want to continue using Snowflake, you can define the transactions schema, connect it to both sources, and internally, ResponsiveML will handle the cutover from one source to another.

    ```python
    transaction_asset = fw.DataAsset(
        schema=TransactionSchema,
        stream_source=KafkaSource(..., timestamp_key="timestamp"),
        batch_source=SnowflakeSource(..., timestamp_key="timestamp"),
        stream_to_batch_schema_map={},
    )
    ```
    [^timestamp]

    [^timestamp]: 
        There are multiple timestamps associated with an event: e.g the time the event is created at the application, the time it’s ingested by the broker. Internally, ResponsiveML will do timestamp resolution to make sure the timestamps are consistent across different sources.

    For example, if you want to count all the transactions up to timestamp T, ResponsiveML will:
    1. first count the transactions in DWH
    2. when that’s done, switch to stream, and continue counting transactions until T

    In ResponsiveML, there are no batch features or stream features, just features.

To create feature logic (feature transformations), you define them on schemas. To compute the actual feature values, the feature transformations have to be applied on data assets. If that’s confusing, don’t worry, we’ll go over an example in the next section.

**Note: ResponsiveML can support dual source data as well as single source data.**

---

## Compute this count feature for all transactions
Instead of computing this feature for a single transaction, you compute it for multiple transactions. It could be because:
- You want to compute this feature to create training data.
- You want to precompute this feature online (e.g. every minute or every time there’s a new transaction). At prediction time, for a given transaction with credit card X, you fetch the latest feature value corresponding to credit card X.

One way to do so is to iterate through every transaction in the training data, extract X and T, and count the transactions for X from the beginning of time up until T.

```python
# Without ResponsiveML
for transaction in tx:
    x = transaction["cc_num"]
    t = transaction["timestamp"]
    count = tx[tx["timestamp"] < t][tx["cc_num"] == x]["cc_num"].count()
```

This operator will get slower and slower as T increases. The reason is that for a given credit card X, each time we process a transaction that involves X, we’ll have to count from the beginning of time. This is stateless processing.

Ideally, we should be able to keep a running count for each credit card X. If at timestamp T, X has been used to make C transactions, the next time we run into a transaction that uses X, we just increment this count by 1. We can keep state by introducing a dictionary that maps from credit_card number to the transaction count. However, this only works if we assume that all transactions are in order, e.g. the transaction with the timestamp T+1 is always processed before the transaction with the timestamp T. It might not always be the case, as some transactions (events) might arrive late.

```python
# Without ResponsiveML: keeping track of count states can be tricky 
# Not using timestamp here b/c we assume that all transactions are in order
count_state = {}
for transaction in tx:
    x = transaction["cc_num"]
    if x not in count_dict:
        count_state[x] = 0
    count_state[x] += 1
```

ResponsiveML handles stateful processing, taking care of out-of-order transactions, and even does so in a distributed manner.

```python
# With ResponsiveML
tx.count(
    window=Window(start=None, end=AT_TIMESTAMP),
    groupby=["cc_num"],
    select=["tid", "timestamp", "cc_num"]
)
```

`tx` is expected to have the same schema as `TransactionSchema`. This will return the same number of records (also known as rows) as tx. Each returned record has 4 fields: "tid", "timestamp", "cc_num", and "count". We can make the input and output schemas explicit as follows:

```python
@schema
class TransactionCountSchema:
    "timestamp": datetime
    "tid": str
    "cc_num": int
    "count": int

@transformation
def tx_count(tx: TransactionSchema) -> TransactionCountSchema:
    return tx.count(
        window=Window(start=None, end=AT_TIMESTAMP),
        groupby=["cc_num"],
        select=["tid", "timestamp", "cc_num"] # select what fields to return
    )
```

Instead of counting the number of transactions for a given credit card from the beginning of time until T, you might want to count the number of transactions 6 months up to T.

```python
@transformation
def tx_count_6_months(tx: TransactionSchema) -> TransactionCountSchema:
    return tx.count(
        window=Window(timedelta(months=6)),
        groupby=["cc_num"],
        select=["tid", "timestamp", "cc_num"]
    )
```

### Register a feature view

Now that we’ve defined feature logic, we want to apply it to a data asset (transactions data) and make available the computed features for low-latency access. We might want to compute this feature online every minute. If we compute it every minute, the feature value’s staleness is up to 1 minute. We can do so as follows:

```python
tx_count_fv = fw.register_feature_view(
    name="tx_count",
    transformation=tx_count,
    data_asset=transaction_asset,
    staleness=timedelta(minute=1),  # resulting from when features are computes
    allowed_lateness=timedelta(minutes=5) 
)
```

---

## Feature consistency for training and prediction

### Low-latency online stores for prediction

All values computed can be immediately made available for low-latency access by your prediction service in an online store backed by Redis.

### Efficient training data generation

To compute this feature to create training data, say for the time period of Sep 2022, you can do so as follows:

```python
tx_count_sep2022 = tx_count_fv.generate_historical_features(
    TimeSpan(
        start="2022-09-01 00:00:00",
        end="2022-09-30 23:59:59"
    )
)
```

You can also do so for multiple features.

```python
feature_set = fw.FeatureSet(name="fraud_model", feature_list=["tx_count_sep2022"]]
training_features = feature_set.generate_historical_features(
    TimeSpan(
        start="2022-09-01 00:00:00",
        end="2022-09-30 23:59:59"
    )
)
```
