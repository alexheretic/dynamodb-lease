mod util;

use anyhow::Context;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, TimeToLiveSpecification,
};
use std::time::Duration;
use time::OffsetDateTime;
use util::*;
use uuid::Uuid;

#[tokio::test]
async fn try_acquire() {
    let lease_table = "test-locker-leases";
    let db_client = localhost_dynamodb().await;
    create_lease_table(lease_table, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();
    // use 2 clients to avoid local locking / simulate distributed usage
    let client2 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("try_acquire:{}", Uuid::new_v4());

    // acquiring a lease should work
    let lease1 = client.try_acquire(&lease_key).await.unwrap();
    assert!(lease1.is_some());

    // subsequent attempts should fail
    let lease2 = client2.try_acquire(&lease_key).await.unwrap();
    assert!(lease2.is_none());
    let lease2 = client2.try_acquire(&lease_key).await.unwrap();
    assert!(lease2.is_none());

    // dropping should asynchronously end the lease
    drop(lease1);

    // in shortish order the key should be acquirable again
    retry::until_ok(|| async {
        client2
            .try_acquire(&lease_key)
            .await
            .and_then(|maybe_lease| maybe_lease.context("did not acquire"))
    })
    .await;
}

#[tokio::test]
async fn local_try_acquire() {
    let lease_table = "test-locker-leases";
    let db_client = localhost_dynamodb().await;
    create_lease_table(lease_table, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("try_acquire:{}", Uuid::new_v4());

    // acquiring a lease should work
    let lease1 = client.try_acquire(&lease_key).await.unwrap();
    assert!(lease1.is_some());

    // subsequent attempts should fail
    let lease2 = client.try_acquire(&lease_key).await.unwrap();
    assert!(lease2.is_none());
    let lease2 = client.try_acquire(&lease_key).await.unwrap();
    assert!(lease2.is_none());

    // dropping should asynchronously end the lease
    drop(lease1);

    // in shortish order the key should be acquirable again
    retry::until_ok(|| async {
        client
            .try_acquire(&lease_key)
            .await
            .and_then(|maybe_lease| maybe_lease.context("did not acquire"))
    })
    .await;
}

#[tokio::test]
#[ignore = "slow"]
async fn try_acquire_extend_past_ttl() {
    let lease_table = "test-locker-leases";
    let db_client = localhost_dynamodb().await;
    create_lease_table(lease_table, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .lease_ttl_seconds(2)
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();
    // use 2 clients to avoid local locking / simulate distributed usage
    let client2 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .lease_ttl_seconds(2)
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("try_acquire_extend_past_expiry:{}", Uuid::new_v4());

    // acquiring a lease should work
    let lease1 = client.try_acquire(&lease_key).await.unwrap();
    assert!(lease1.is_some());

    // subsequent attempts should fail
    assert!(client2.try_acquire(&lease_key).await.unwrap().is_none());

    // after some time the original lease will have expired
    // however, a background task should have extended it so it should still be active.
    // Note: Need to wait ages to reliably trigger ttl deletion :(
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(
        client2.try_acquire(&lease_key).await.unwrap().is_none(),
        "lease should have been extended"
    );
}

#[tokio::test]
async fn acquire() {
    let lease_table = "test-locker-leases";
    let db_client = localhost_dynamodb().await;
    create_lease_table(lease_table, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();
    // use 2 clients to avoid local locking / simulate distributed usage
    let client2 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("acquire:{}", Uuid::new_v4());

    // acquiring a lease should work
    let lease1 = client.acquire(&lease_key).await.unwrap();

    // subsequent attempts should fail
    let lease2 =
        tokio::time::timeout(Duration::from_millis(100), client2.acquire(&lease_key)).await;
    assert!(lease2.is_err(), "should not acquire while lease1 is alive");

    // dropping should asynchronously end the lease
    drop(lease1);

    // in shortish order the key should be acquirable again
    tokio::time::timeout(TEST_WAIT, client2.acquire(&lease_key))
        .await
        .expect("could not acquire after drop")
        .expect("failed to acquire");
}

#[tokio::test]
async fn release_try_acquire() {
    let lease_table = "test-locker-leases";
    let db_client = localhost_dynamodb().await;
    create_lease_table(lease_table, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();
    // use 2 clients to avoid local locking / simulate distributed usage
    let client2 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("acquire:{}", Uuid::new_v4());

    // acquiring a lease should work
    let lease1 = client.acquire(&lease_key).await.unwrap();

    // subsequent attempts should fail
    assert!(
        client2.try_acquire(&lease_key).await.unwrap().is_none(),
        "should not be able to acquire while lease1 is alive"
    );

    // Release the lease and await deletion
    lease1.release().await.unwrap();

    // now another client can immediately acquire
    client2
        .try_acquire(lease_key)
        .await
        .unwrap()
        .expect("failed to acquire after release");
}

#[tokio::test]
async fn local_acquire() {
    let lease_table = "test-locker-leases";
    let db_client = localhost_dynamodb().await;
    create_lease_table(lease_table, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("acquire:{}", Uuid::new_v4());

    // acquiring a lease should work
    let lease1 = client.acquire(&lease_key).await.unwrap();

    // subsequent attempts should fail
    let lease2 = tokio::time::timeout(Duration::from_millis(100), client.acquire(&lease_key)).await;
    assert!(lease2.is_err(), "should not acquire while lease1 is alive");

    // dropping should asynchronously end the lease
    drop(lease1);

    // in shortish order the key should be acquirable again
    tokio::time::timeout(TEST_WAIT, client.acquire(&lease_key))
        .await
        .expect("could not acquire after drop")
        .expect("failed to acquire");
}

#[tokio::test]
async fn acquire_timeout() {
    let lease_table = "test-locker-leases";
    let db_client = localhost_dynamodb().await;
    create_lease_table(lease_table, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();
    // use 2 clients to avoid local locking / simulate distributed usage
    let client2 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("acquire:{}", Uuid::new_v4());

    // acquiring a lease should work
    let lease1 = client
        .acquire_timeout(&lease_key, Duration::from_millis(100))
        .await
        .unwrap();

    // subsequent attempts should fail
    let lease2 = client2
        .acquire_timeout(&lease_key, Duration::from_millis(100))
        .await;
    assert!(lease2.is_err(), "should not acquire while lease1 is alive");

    // dropping should asynchronously end the lease
    drop(lease1);

    // in shortish order the key should be acquirable again
    client2
        .acquire_timeout(&lease_key, TEST_WAIT)
        .await
        .expect("failed to acquire");
}

/// Acquiring should work if an expired lease already exists. This could be
/// caused by a lease holder crashing or losing connection to the db.
#[tokio::test]
async fn acquire_expired_lease() {
    const KEY_FIELD: &str = "key";
    const LEASE_EXPIRY_FIELD: &str = "lease_expiry";
    const LEASE_VERSION_FIELD: &str = "lease_version";
    const LEASE_TABLE: &str = "test-acquire-expired-lease";

    let db_client = localhost_dynamodb().await;
    create_lease_table(LEASE_TABLE, &db_client).await;

    let client = dynamodb_lease::Client::builder()
        .table_name(LEASE_TABLE)
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();

    // disable ttl to ensure local test dynamodb does **not** actually reap the outdated record
    // to simulate documented behaviour
    // > DynamoDB automatically deletes expired items within a few days of their expiration time
    // <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html>
    db_client
        .update_time_to_live()
        .table_name(LEASE_TABLE)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(false)
                .attribute_name("lease_expiry")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let lease_key = format!("acquire_expired_lease:{}", Uuid::new_v4());

    // manually create an expired lease record to simulate this state
    let expired_lease_v = Uuid::new_v4();
    let before_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    db_client
        .put_item()
        .table_name(LEASE_TABLE)
        .item(KEY_FIELD, AttributeValue::S(lease_key.clone()))
        .item(
            LEASE_EXPIRY_FIELD,
            AttributeValue::N((before_timestamp - 1).to_string()),
        )
        .item(
            LEASE_VERSION_FIELD,
            AttributeValue::S(expired_lease_v.to_string()),
        )
        .send()
        .await
        .unwrap();

    // acquiring a lease should work immediately, overwriting the expired lease
    let _lease = client
        .try_acquire(&lease_key)
        .await
        .expect("Could not acquire")
        .expect("try_acquire returned None");

    // check db lease is correct
    let get = db_client
        .get_item()
        .table_name(LEASE_TABLE)
        .key(KEY_FIELD, AttributeValue::S(lease_key.clone()))
        .send()
        .await
        .unwrap();
    let rec = get.item().unwrap();
    // the lease expiry should no longer be expired
    let rec_expiry = rec[LEASE_EXPIRY_FIELD]
        .as_n()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(rec_expiry > before_timestamp, "Unexpected record {rec:?}");

    // the lease version should have changed from the expired one
    let rec_lease_v = rec[LEASE_VERSION_FIELD].as_s().unwrap();
    assert_ne!(
        rec_lease_v,
        &expired_lease_v.to_string(),
        "Unexpected record {rec:?}"
    );
}

#[tokio::test]
async fn init_should_check_table_exists() {
    let db_client = localhost_dynamodb().await;

    let err = dynamodb_lease::Client::builder()
        .table_name("test-locker-leases-not-exists")
        .build_and_check_db(db_client)
        .await
        .expect_err("should check table exists");
    assert!(
        err.to_string().to_ascii_lowercase().contains("missing"),
        "{}",
        err
    );
}

#[tokio::test]
async fn init_should_check_hash_key() {
    let table_name = "table-with-wrong-key";
    let db_client = localhost_dynamodb().await;

    let _ = db_client
        .create_table()
        .table_name(table_name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("wrong")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("wrong")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .send()
        .await;

    let err = dynamodb_lease::Client::builder()
        .table_name(table_name)
        .build_and_check_db(db_client)
        .await
        .expect_err("should check hash 'key'");
    assert!(
        err.to_string().to_ascii_lowercase().contains("key"),
        "{}",
        err
    );
}

#[tokio::test]
async fn init_should_check_hash_key_type() {
    let table_name = "table-with-wrong-key-type";
    let db_client = localhost_dynamodb().await;

    let _ = db_client
        .create_table()
        .table_name(table_name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("key")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("key")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .send()
        .await;

    let err = dynamodb_lease::Client::builder()
        .table_name(table_name)
        .build_and_check_db(db_client)
        .await
        .expect_err("should check hash key type");
    assert!(
        err.to_string().to_ascii_lowercase().contains("type"),
        "{}",
        err
    );
}

#[tokio::test]
async fn init_should_check_ttl() {
    let table_name = "table-with-without-ttl";
    let db_client = localhost_dynamodb().await;

    let _ = db_client
        .create_table()
        .table_name(table_name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("key")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("key")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .send()
        .await;

    let err = dynamodb_lease::Client::builder()
        .table_name(table_name)
        .build_and_check_db(db_client)
        .await
        .expect_err("should check ttl");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("time to live"),
        "{}",
        err
    );
}
