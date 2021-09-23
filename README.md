MPI
====
- [MPI](#mpi)
  - [Overview](#overview)
  - [Implementation](#implementation)
    - [Order Manifest](#order-manifest)
    - [Cooldown](#cooldown)
    - [Persistence](#persistence)
    - [Publish Delay or Service Failure](#publish-delay-or-service-failure)
  - [Running the Project](#running-the-project)
    - [Configure](#configure)
    - [Installing](#installing)
    - [Run](#run)
    - [Tests](#tests)

## Overview
Pi's role is to keep track of active customers in a cache, and regularly publish the `state` of this cache to a downstream application. Customers are identified in the cache by their phone number, and are added to the cache when a `start` message is received. Once they are added, they remain active until one of the following conditions is met.
- a `stop` message is received for the same phone number
- their data ages off after the configured `active_time`
- there is more active records than is allowed by the `manifest_size`, and the customer is sent to cooldown, as they have already been included in at least one manifest

- after enforcing cooldown, there is still too many active records, and this customer is one of the oldest active records

## Implementation
The following examples have dummy data for ease of reading, but all the following fields are configurable.
:`cooldown_time`
:`manifest_size`
:`active_time`
:`refresh_time`


### Order Manifest
The order manifest is made up of `100` customer records. To be included in a manifest a customer must:
- have had a start message for them come in during the last `60` seconds
- not have had a stop message for them received since they were added to the cache
- not be in cooldown

The order manifest is published once every `20` seconds, and each one overwrites the previous one. The manifest should contain only the records which should be active on the client.


### Cooldown
A customer can be forced into cooldown if the amount of active orders in the manifest is greater than the allowed amount. Customers only end up in cooldown if they have recently been included in a published manifest. The general idea is to allow us to rotate through customers, and avoid situations where the same customers end up ordering every time. Cooldown time is able to be configured, and is only enforced in situations where we have too many active customers at once. Once the cooldown time has expired, customers are removed from the cache so they can re-order next time a start comes in for them.

One of the primary design considerations, was how to keep the cache as full as possible. To this end, cooldown is only enforced when the amount of active records is higher than the allowed `manifest_size`. If we drop below this limit, customers will be released from cooldown early.


### Persistence
PI currently does not persist any data between sessions. The cache is built and stored only in memory for as long as the connection to it is open, and ceases to exist as soon as the database connection is closed. This is tied to the start and stop methods in the service. Due to the small time windows, it is impractical to assume that any data saved to disk before a service restart or failure would still be current.


### Publish Delay or Service Failure
The client at the remote destination must handle unexpected delays in receiving the order list. It is responsible for clearing the current orders if updates are not received from PI within its allowed time window.

## Running the Project
### Configure
You do not have to do this step, if you intend to use the Docker environment.
```bash
cp config.yml.example config.yml 
```

### Installing
```
# creates virtual environment and installs package
make install
```

### Run
```
mpi
```

### Tests
To run the tests:
```
make test
```