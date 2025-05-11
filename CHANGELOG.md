## 0.22.1
* Overwrite expired leases on acquire. This handles the case a crashed lease holder
  has not cleaned up db state faster than waiting for db ttl cleanup.
* Add `Lease::is_healthy` fn.
* Add `Lease::release` fn.

## 0.22.0
* Update _aws-sdk-dynamodb_ to `1.1`.

## 0.21.0
* Update _aws-sdk-dynamodb_ to `0.39`.

## 0.20.0
* Update _aws-sdk-dynamodb_ to `0.35`.

## 0.19.0
* Update _aws-sdk-dynamodb_ to `0.34`.

## 0.18.0
* Update _aws-sdk-dynamodb_ to `0.33`.

## 0.17.0
* Update _aws-sdk-dynamodb_ to `0.32`.

## 0.16.0
* Update _aws-sdk-dynamodb_ to `0.31`.
* Remove feature `native-tls` (removed upstream).

## 0.15.0
* Update _aws-sdk-dynamodb_ to `0.28`.

## 0.14.0
* Update _aws-sdk-dynamodb_ to `0.27`.

## 0.13.0
* Update _aws-sdk-dynamodb_ to `0.25`.

## 0.12.0
* Update _aws-sdk-dynamodb_ to `0.24`.

## 0.11.0
* Update _aws-sdk-dynamodb_ to `0.23`.

## 0.10.0
* Update _aws-sdk-dynamodb_ to `0.22`.

## 0.9.0
* Update _aws-sdk-dynamodb_ to `0.21`.

## 0.8.0
* Update _aws-sdk-dynamodb_ to `0.19`.

## 0.7.0
* Update _aws-sdk-dynamodb_ to `0.18`.

## 0.6.0
* Update _aws-sdk-dynamodb_ to `0.17`.

## 0.5.0
* Update _aws-sdk-dynamodb_ to `0.16`.

## 0.4.1
* Add _tracing_ support.
* Tweak acquisition logic to remove an unfair advantage if just dropped a lease that could
  starve remote acquisition attempts under high contention.

## 0.4.0
* Update _aws-sdk-dynamodb_ to `0.15`.

## 0.3.0
* Update _aws-sdk-dynamodb_ to `0.14`.

## 0.2.0
* Update _aws-sdk-dynamodb_ to `0.13`.

## 0.1.0
* Add `dynamodb_lease::Client` & friends.
