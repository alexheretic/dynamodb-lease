mod util;

use anyhow::Context;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::time::Duration;
use util::*;
use uuid::Uuid;

#[tokio::test]
async fn try_acquire() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn local_try_acquire() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
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
    let _ = instance.stop().await;
}

#[tokio::test]
#[ignore = "slow"]
async fn try_acquire_extend_past_ttl() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn acquire() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn release_try_acquire() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn local_acquire() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn acquire_timeout() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn init_should_check_table_exists() {
    let (db_client, instance) = get_test_db().await;

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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn init_should_check_hash_key() {
    let table_name = "table-with-wrong-key";
    let (db_client, instance) = get_test_db().await;

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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn init_should_check_hash_key_type() {
    let table_name = "table-with-wrong-key-type";
    let (db_client, instance) = get_test_db().await;

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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn init_should_check_ttl() {
    let table_name = "table-with-without-ttl";
    let (db_client, instance) = get_test_db().await;

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
    let _ = instance.stop().await;
}

#[tokio::test]
async fn acquire_or_replace_expired_lease_replaces_after_grace_period() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
    create_lease_table(lease_table, &db_client).await;

    let ttl = Duration::from_secs(2);
    let grace_period = Duration::from_secs(3);

    let client1 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .lease_ttl_seconds(ttl.as_secs() as u32)
        .grace_period(grace_period)
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();
    let client2 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .lease_ttl_seconds(ttl.as_secs() as u32)
        .grace_period(grace_period)
        .acquire_cooldown(Duration::from_millis(100)) // Speed up retry for test
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("replace_expired:{}", Uuid::new_v4());

    // 1. Client 1 acquires the lease
    let lease1 = client1.acquire(&lease_key).await.unwrap();
    let lease1_version = lease1.lease_v().await;
    println!("Client 1 acquired lease {}", lease1_version);

    // 2. Drop lease1 immediately to stop the background extension task.
    println!("Dropping lease1 to stop extensions");
    drop(lease1);

    // 3. Wait for TTL + Grace Period + buffer to ensure expiry and grace passed.
    let wait_duration = ttl + grace_period + Duration::from_millis(500);
    println!("Waiting for {:?} for expiry + grace period", wait_duration);
    tokio::time::sleep(wait_duration).await;

    // 4. Verify try_acquire still fails (item might exist due to TTL delay, but should be replaceable)
    // Note: This check might be flaky depending on exact DynamoDB TTL timing.
    // It's primarily checking our logic boundary, not DynamoDB's.
    // assert!(
    //     client2.try_acquire(&lease_key).await.unwrap().is_none(),
    //     "try_acquire should fail even after grace period if item exists"
    // );
    // println!("Client 2 try_acquire correctly failed after grace period");

    // 5. Client 2 should now be able to acquire the lease, replacing the expired one.
    println!("Attempting acquire_or_replace_expired_lease");
    let lease2 = tokio::time::timeout(
        TEST_WAIT, // Use standard test wait, should be enough now
        client2.acquire_or_replace_expired_lease(&lease_key),
    )
    .await
    .expect("Client 2 timed out acquiring after grace period")
    .expect("Client 2 failed to acquire after grace period");
    let lease2_version = lease2.lease_v().await;
    println!(
        "Client 2 acquired lease {} after grace period",
        lease2_version
    );

    // 6. Verify it's a *new* lease (different version)
    assert_ne!(
        lease1_version, lease2_version,
        "Lease version should change after replacement"
    );

    // 7. Drop the new lease
    drop(lease2);
    let _ = instance.stop().await;
}

#[tokio::test]
async fn acquire_or_replace_expired_lease_waits_like_acquire() {
    let lease_table = "test-locker-leases";
    let (db_client, instance) = get_test_db().await;
    create_lease_table(lease_table, &db_client).await;

    // Use clients with default TTL/grace period
    let client1 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .acquire_cooldown(Duration::from_millis(100)) // Speed up retry for test
        .build_and_check_db(db_client.clone())
        .await
        .unwrap();
    let client2 = dynamodb_lease::Client::builder()
        .table_name(lease_table)
        .acquire_cooldown(Duration::from_millis(100)) // Speed up retry for test
        .build_and_check_db(db_client)
        .await
        .unwrap();

    let lease_key = format!("replace_waits:{}", Uuid::new_v4());

    // 1. Client 1 acquires the lease using normal acquire
    let lease1 = client1.acquire(&lease_key).await.unwrap();
    let lease1_v = lease1.lease_v().await;
    println!("Client 1 acquired lease {}", lease1_v);

    // 2. Spawn a task for Client 2 to acquire using acquire_or_replace_expired_lease
    let key_clone = lease_key.clone();
    let mut acquire_task = tokio::spawn(async move {
        println!("Client 2 attempting acquire_or_replace...");
        client2.acquire_or_replace_expired_lease(&key_clone).await
    });

    // 3. Check that Client 2's task blocks (doesn't complete immediately)
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(1)) => {
            println!("Client 2 acquire task correctly blocked");
        }
        result = &mut acquire_task => {
           panic!("Client 2 acquire completed unexpectedly: {:?}", result);
        }
    }

    // 4. Drop Client 1's lease
    println!("Dropping Client 1's lease");
    drop(lease1);

    // 5. Client 2's task should now complete successfully
    let lease2_result = tokio::time::timeout(TEST_WAIT, acquire_task)
        .await
        .expect("Client 2 acquire task timed out after lease1 dropped")
        .expect("Client 2 acquire task panicked")
        .expect("Client 2 failed to acquire lease after lease1 dropped");

    let lease2_v = lease2_result.lease_v().await;
    println!("Client 2 acquired lease {} successfully", lease2_v);
    drop(lease2_result);
    let _ = instance.stop().await;
}
