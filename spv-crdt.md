# Layering SPV Payments into Merkle-CRDT Replication with GossipSub

This document explores an approach for integrating SPV (Simplified Payment Verification) payments into the Merkle-CRDT replication system that uses GossipSub. The primary objective is to enable scenarios like paid subscriptions to CRDT data feeds or to compensate peers acting as paid store-and-forward service providers.

## Core Principles

1.  **Access Control via Payment:** Access to specific CRDT data updates or related services (like data retrieval or relaying) will be conditional upon proof of payment or an active subscription.
2.  **SPV for Payment Verification:** Bitcoin SV SPV payments will be used for making and verifying these payments between peers efficiently.
3.  **Layered Architecture:** Payment logic will be integrated at appropriate layers of the existing Merkle-CRDT-GossipSub stack, aiming to keep core protocols like GossipSub generic.

## Proposed Scenarios and Mechanisms

We will primarily focus on enabling paid subscriptions to CRDT topics/instances and compensating peers for data synchronization services.

### Scenario 1: Paid Subscription to a CRDT Topic/Instance

**Objective:** Peers pay a recurring fee to receive "AnnounceHeads" messages (and subsequently fetch data) for a specific CRDT instance disseminated via a GossipSub topic.

**Mechanism:**

1.  **Subscription Management Protocol:**
    *   A dedicated libp2p protocol (e.g., `/crdt-subscription/1.0.0`) will manage the subscription lifecycle.
    *   **Flow:**
        1.  A **Subscriber** peer sends a "SubscriptionRequest" (including target CRDT ID, desired duration) to the **Publisher** (owner/provider of the CRDT data).
        2.  The Publisher responds with an SPV `InvoiceRequest` detailing the subscription fee.
        3.  The Subscriber makes the SPV `Payment` (transmitting BEEF data) to the Publisher.
        4.  The Publisher verifies the SPV `Payment`. Upon success, it sends a `PaymentAcknowledgment` and issues an **"AccessToken"** (e.g., a digitally signed, time-limited token containing `subscriberPeerId`, `crdtInstanceId`, `expiryDate`) to the Subscriber.
    *   This "AccessToken" serves as proof of an active subscription.

2.  **GossipSub Integration (for "AnnounceHeads"):**
    *   **Publisher's Responsibility:** The Publisher of the CRDT data, when using `PubSub.publish()` to send "AnnounceHeads" messages, will primarily manage its GossipSub mesh (`params.D`, `DLow`, `DHigh`) to include peers with valid "AccessTokens" for that CRDT.
        *   Non-paying peers might still receive "AnnounceHeads" via GossipSub's ambient gossip (`DLazy`), but they won't be able to fetch the actual data without a valid token (see next point).
    *   **GossipSub Message Validation (Optional Enhancement):** The `messageValidator` for PubSub could be used by the Publisher to verify that only authorized peers (e.g., the owner) are *publishing* "AnnounceHeads" messages to a protected topic, preventing spam or unauthorized announcements. This controls injection, not consumption.

3.  **DAG Sync Protocol (`/crdt-dag-sync/1.0.0`) Enhancement for Paid Access:**
    *   When a peer (Subscriber) receives an "AnnounceHeads" message via GossipSub and needs to fetch the actual DAG nodes:
        *   It includes its valid "AccessToken" in the `/crdt-dag-sync/1.0.0` request message to the peer serving the data (Publisher or a designated relay).
        *   **Request Message Example:** `{ crdtInstanceId: "X", wantCids: ["CID1"], accessToken: "UserA_Token_for_X" }`.
    *   The peer serving the DAG nodes (the **Data Provider**) verifies the "AccessToken" (checks signature, expiry, and if it matches the requested `crdtInstanceId`).
    *   If the token is valid, the Data Provider serves the requested DAG nodes. Otherwise, it can refuse the request or respond with an error.

### Scenario 2: Paid Store-and-Forward / Relay Services

**Objective:** Dedicated peers (Relays) offer to store CRDT data and provide reliable access or wider dissemination for a fee.

**Mechanism:**

1.  **Service Agreement & Payment:**
    *   A client peer negotiates service terms with a Relay peer using a specific protocol (e.g., `/crdt-relay-agreement/1.0.0`).
    *   SPV payments are made for the relay service (e.g., for storage duration, data transfer volume, or a flat fee for ensuring availability). The Relay provides a service token upon successful payment.
2.  **Relay Behavior:**
    *   **GossipSub Participation:** The Relay subscribes to the CRDT topics it's paid to service. It actively participates in the GossipSub mesh to receive "AnnounceHeads" messages and may also re-publish or gossip them further to enhance reach.
    *   **DAG Sync Service:** The Relay acts as a Data Provider for the `/crdt-dag-sync/1.0.0` protocol. When a peer requests DAG nodes, the Relay checks for a valid service token (from the client who paid the Relay) or if the requester is directly paying the Relay for this specific sync operation.

### General SPV Integration Components

*   **SPV Protocol Module:** A reusable module within the application (similar to `computonium_ui/SPVProtocol`) will be responsible for:
    *   Generating SPV `InvoiceRequest` messages.
    *   Constructing and signing SPV `Payment` transactions (resulting in BEEF data).
    *   Validating received SPV `Payment` (verifying BEEF data, including Merkle proofs against a `BlockHeaderService`).
    *   Sending `PaymentAcknowledgment` messages.
*   **Required Services:** The system will need access to:
    *   `WalletProvider`: For managing BSV keys and signing transactions.
    *   `BlockHeaderService`: For validating Merkle proofs in SPV payments.
    *   Transaction Broadcasting Service (e.g., `ArcService` or `WhatsOnChainService`): For submitting SPV payment transactions to the BSV network.

## Protocol Stack with SPV Payments

This diagram illustrates the various protocol layers involved when SPV payments are integrated with the Merkle-CRDT replication system over GossipSub:

```text
+-------------------------------------------------+
|      SPV Payment Logic & CRDT Application       |  <-- Application & Payment Logic Layer
+-------------------------------------------------+
       | | |
       | | +------------> 'AnnounceHeads' Messages (on GossipSub Topic)
       | +--------------> DAG Sync Protocol (/crdt-dag-sync/1.0.0) (verifies AccessToken)
       +----------------> Subscription Mgmt Protocol (/crdt-subscription/1.0.0)
+-------------------------------------------------+
|      CRDT & Subscription Service Layer          |
+-------------------------------------------------+
       |         |         |
       |         |         +--> GossipSub Topic (e.g., /crdt/InstanceID/updates)
       |         |
       (direct libp2p stream)   (direct libp2p stream)
       |         |
+-------------------------------------------------+
|      Pub/Sub Routing Layer (GossipSub)          |
+-------------------------------------------------+
       |
+-------------------------------------------------+
|      Libp2p Core Stack                          |
|      (Multiplexing - Yamux, Security - Noise,   |
|       Transport - TCP/UDP)                      |  <-- Libp2p Core Network Layer
+-------------------------------------------------+
```
*   **Application & Payment Logic Layer:** Contains the user-facing CRDT application logic and the SPV payment processing (creating invoices, making payments, verifying payments).
*   **CRDT & Subscription Service Layer:**
    *   Handles the mechanics of paid subscriptions (`/crdt-subscription/1.0.0`).
    *   Manages data synchronization (`/crdt-dag-sync/1.0.0`), gating access based on tokens.
    *   Formats and publishes "AnnounceHeads" messages.
*   **Pub/Sub Routing Layer:** GossipSub disseminates "AnnounceHeads" messages on specific topics.
*   **Libp2p Core Network Layer:** Provides the underlying secure, multiplexed P2P connections.

## Application-Level Policy and Peer Scoring

*   **Access Control:** Primarily enforced at the application level (e.g., by the `P2PCrdtService` or the handlers for the subscription and DAG sync protocols) by checking "AccessTokens" or direct payment proofs.
*   **GossipSub Peer Scoring (`appSpecificScore`):**
    *   **Reward:**
        *   Peers maintaining active, valid subscriptions.
        *   Publishers/Relays that reliably provide data to authorized subscribers.
        *   Peers that promptly make SPV payments when required.
    *   **Penalize:**
        *   Peers attempting unauthorized access to paid data.
        *   Peers frequently failing SPV payment processes.
        *   Relays that fail to provide data they are paid to serve.
    *   This helps the GossipSub network self-organize to favor economically aligned and well-behaved participants.

## Illustrative Workflow (Paid Subscription Model)

1.  **Subscription:**
    *   Peer S (Subscriber) wishes to access CRDT `C1` managed by Peer P (Publisher).
    *   S initiates `/crdt-subscription/1.0.0` with P.
    *   P sends SPV `InvoiceRequest` to S.
    *   S makes SPV `Payment` (BEEF) to P.
    *   P verifies payment, sends `PaymentAcknowledgment` and `AccessToken_C1_S` to S.
2.  **GossipSub Activity:**
    *   S subscribes to GossipSub topic `/crdt/C1/updates`.
    *   P publishes "AnnounceHeads: { crdtId: C1, heads: [H_new] }" to this topic. P's GossipSub logic may prioritize meshing with S due to the active subscription.
3.  **Data Retrieval:**
    *   S receives the announcement for `H_new`.
    *   S initiates `/crdt-dag-sync/1.0.0` with P (or a designated relay).
    *   S includes `AccessToken_C1_S` in the sync request for `H_new`.
    *   P verifies `AccessToken_C1_S`. If valid, P sends the DAG node data for `H_new`.
    *   S integrates the new data into its local CRDT replica.

```text
Subscriber (S)             Publisher (P)         GossipSub Network (GSNet)    SPV Payment System (BSVNet)
    |                             |                         |                             |
    | 1. SubscriptionRequest      |                         |                             |
    | (/crdt-subscription/1.0.0)  |                         |                             |
    | for CRDT C1                 |                         |                             |
    |---------------------------->|                         |                             |
    |                             |                         |                             |
    |      2. SPV InvoiceRequest  |                         |                             |
    |<----------------------------|                         |                             |
    |                             |                         |                             |
    |                             |         3. Makes SPV Payment (constructs BEEF)        |
    |------------------------------------------------------------------------------------->|
    |                             |                         |                             |
    | [Obtains Payment BEEF data] |                         |                             |
    |                             |                         |                             |
    | 4. SPV Payment (BEEF data)  |                         |                             |
    |---------------------------->|                         |                             |
    |                             |                         |                             |
    |                             |       5. Verifies Payment (using BEEF & Block Headers)|
    |                             |------------------------------------------------------->|
    |                             |                         |                             |
    | 6. PaymentAcknowledgment &  |                         |                             |
    |    AccessToken_C1_S         |                         |                             |
    |<----------------------------|                         |                             |
    |                             |                         |                             |
    | [Subscribes to CRDT C1 topic via PubSub] |            |                             |
    |                             |                         |                             |
    |      7. PubSub.subscribe("/crdt/C1/updates")        |                             |
    |------------------------------------------------------>|                             |
    |                             |                         |                             |
    |                             | [Local CRDT C1 update occurs] |                       |
    |                             |                         |                             |
    |                             | 8. PubSub.publish("AnnounceHeads: {crdtId: C1, heads: [H_new]}") |
    |                             |------------------------>|                             |
    |                             |                         |                             |
    |      9. Receives "AnnounceHeads" for H_new            |                             |
    |<------------------------------------------------------|                             |
    |                             |                         |                             |
    | [Determines H_new (and possibly ancestors) is missing] | |                           |
    |                             |                         |                             |
    | 10. DAG Sync Request        |                         |                             |
    | (/crdt-dag-sync/1.0.0) for  |                         |                             |
    | H_new (includes AccessToken_C1_S) |                   |                             |
    |---------------------------->|                         |                             |
    |                             |                         |                             |
    |                             | 11. Verifies AccessToken_C1_S (valid, signed, not expired) |
    |                             |<------------------------|                             |
    |                             |                         |                             |
    |                             | 12. Sends DAG Node Data for H_new (and any other requested valid nodes) |
    |                             |                         |                             |
    |<----------------------------|                         |                             |
    |                             |                         |                             |
    | [Integrates H_new into local CRDT replica] |          |                             |
    |                             |                         |                             |
```

## Summary

Layering SPV payments involves creating specific application-level protocols for subscription management and paid data access, which then interact with the GossipSub-based CRDT announcement system and the direct DAG sync mechanism. Access control is primarily enforced by verifying payment proofs (like "AccessTokens") before granting access to data via the DAG sync protocol. GossipSub's peer scoring can further reinforce the economic incentives and reliability of the system. This approach keeps the core GossipSub protocol unchanged while enabling monetization of the CRDT data and services built on top.
