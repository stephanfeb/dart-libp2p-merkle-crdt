# Proposal: Integrating Bitcoin Payments into Merkle-CRDT Replication

## 1. Introduction

This document proposes two primary approaches for integrating Bitcoin (BSV) payments into the `dart-libp2p-merkle-crdt` project. The goal is to enable monetization of CRDT data feeds and associated services, such as paid subscriptions or per-use access. The two approaches are:

1.  **SPV Payment for Subscriptions with Access Tokens:** Peers make discrete SPV payments to subscribe to data feeds for a defined period, receiving an access token to authorize data retrieval.
2.  **Payment Channels for Streaming Payments:** Peers establish payment channels for making frequent, low-cost, off-chain micropayments, suitable for fine-grained, usage-based access to data or services.

This proposal details the mechanisms, components, advantages, disadvantages, and integration points for each approach.

## 2. Approach 1: SPV Payment for Subscriptions with Access Tokens

This approach is largely based on the concepts outlined in the `spv-crdt.md` document.

### 2.1. Overview

Subscribers pay a recurring fee using Bitcoin SV SPV (Simplified Payment Verification) payments to gain access to specific CRDT data updates or services for a predetermined duration. Upon successful payment, the subscriber receives a digitally signed, time-limited "AccessToken" which is presented to access protected resources.

### 2.2. Mechanism

1.  **Subscription Management Protocol:**
    *   A dedicated libp2p protocol (e.g., `/crdt-subscription/1.0.0`) manages the subscription lifecycle.
    *   **Flow:**
        1.  A **Subscriber** sends a "SubscriptionRequest" (including target CRDT ID, desired duration) to the **Publisher**.
        2.  The Publisher responds with an SPV `InvoiceRequest` detailing the subscription fee.
        3.  The Subscriber makes an SPV `Payment` (transmitting BEEF data) to the Publisher.
        4.  The Publisher verifies the SPV `Payment`. Upon success, it sends a `PaymentAcknowledgment` and issues an **"AccessToken"** (e.g., a digitally signed, time-limited token containing `subscriberPeerId`, `crdtInstanceId`, `expiryDate`) to the Subscriber.

2.  **AccessToken Usage:**
    *   The "AccessToken" serves as proof of an active subscription.
    *   It is included in requests to access protected data, primarily via the DAG Sync protocol.

3.  **DAG Sync Protocol (`/crdt-dag-sync/1.0.0`) Enhancement:**
    *   When a Subscriber requests DAG nodes, it includes its valid "AccessToken" in the request.
    *   The **Data Provider** (Publisher or Relay) verifies the "AccessToken" (signature, expiry, permissions) before serving the data.

### 2.3. Required Components

*   **SPV Protocol Module:** A local library/module for:
    *   Generating SPV `InvoiceRequest` messages.
    *   Constructing and signing SPV `Payment` transactions (resulting in BEEF data).
    *   Validating received SPV `Payment` (verifying BEEF data against a `BlockHeaderService`).
    *   Sending `PaymentAcknowledgment` messages.
*   **WalletProvider:** Manages BSV keys and signs transactions.
*   **BlockHeaderService:** Provides block headers for SPV proof validation (can be a trusted remote service or a local cache).
*   **Transaction Broadcasting Service:** Submits SPV payment transactions to the BSV network (e.g., WhatsOnChain, Arc).
*   **Subscription Management Service:** Implements the `/crdt-subscription/1.0.0` protocol logic, issues and potentially tracks AccessTokens.

### 2.4. Pros

*   **Relatively Simpler Model:** Conceptually straightforward, leveraging direct on-chain payments for access periods.
*   **Clear Access Control:** Token-based access is a well-understood pattern.
*   **Leverages SPV:** Utilizes SPV for efficient on-chain payment verification without needing a full node.
*   **Defined Service Periods:** Payments are tied to specific subscription durations.

### 2.5. Cons

*   **Payment Granularity:** Less suitable for very fine-grained, per-use payments due to the overhead of an on-chain transaction per subscription period.
*   **Transaction Fees:** Each subscription payment incurs a BSV network transaction fee.
*   **Latency:** Payment confirmation and token issuance depend on BSV network confirmation times (though SPV with BEEF can speed this up considerably for the payee).

### 2.6. Integration Points in `dart-libp2p-merkle-crdt`

*   **New Module `lib/src/spv/`:** Houses the `SPV Protocol Module` logic.
*   **New Module `lib/src/subscription_manager/`:**
    *   Implements the libp2p stream handler for `/crdt-subscription/1.0.0`.
    *   Manages AccessToken generation and validation logic.
*   **Enhance `lib/src/gossip_manager/p2p_stream_dag_fetcher.dart` (and its server-side counterpart):**
    *   Client-side: Modify to send `accessToken` in requests.
    *   Server-side: Add logic to verify `accessToken` before responding with data.
*   **Application Layer (e.g., `lib/src/dart_libp2p_merkle_crdt_base.dart` or a new service):**
    *   Orchestrates the interaction between CRDT services, subscription management, and SPV logic.
    *   Manages which CRDT instances require paid subscriptions.

## 3. Approach 2: Payment Channels for Streaming Payments

This approach enables more frequent, lower-cost payments suitable for continuous or metered access to data/services.

### 3.1. Overview

Peers establish a bilateral payment channel by committing funds to an on-chain multi-signature address. Once established, they can exchange numerous off-chain "micro-payments" that update the channel's balance distribution. These off-chain payments are fast and incur no individual network transaction fees. The channel is settled on-chain when the service relationship ends or by agreement.

### 3.2. Mechanism

1.  **Channel Establishment (On-Chain):**
    *   The **Payer** (e.g., Subscriber) and **Payee** (e.g., Publisher) negotiate channel terms (e.g., initial funding amounts).
    *   Both parties collaboratively create and fund a 2-of-2 multi-signature BSV address. This funding transaction is broadcast to the BSV network and can be verified via SPV.
    *   A dedicated libp2p protocol (e.g., `/payment-channel/1.0.0`) would manage this negotiation and setup.

2.  **Off-Chain Micro-Payments:**
    *   Over the `/payment-channel/1.0.0` libp2p stream (or a related one), the Payer sends signed messages to the Payee representing incremental payments.
    *   These messages update the agreed-upon balance split of the funds held in the multi-signature address. For example, "Payer transfers 100 satoshis to Payee from channel balance."
    *   These messages are not broadcast to the BSV network until channel settlement.

3.  **Service Delivery Tied to Off-Chain Updates:**
    *   The Payee provides access to data or services incrementally upon receiving valid, signed off-chain payment updates.
    *   For example, for every X messages processed, Y data blocks delivered, or Z seconds of connection time, an off-chain payment is made.

4.  **Channel Settlement (On-Chain):**
    *   When the service concludes, or by mutual agreement/timeout, the channel is closed.
    *   The peers collaboratively create and sign a final settlement transaction that distributes the channel funds according to the last agreed-upon off-chain state. This transaction is broadcast to the BSV network.
    *   Dispute resolution mechanisms (e.g., using pre-signed but time-locked refund transactions) might be needed if one party becomes unresponsive.

### 3.3. Required Components

*   **Payment Channel Management Module:**
    *   Implements the libp2p stream handler for `/payment-channel/1.0.0`.
    *   Manages channel negotiation, state tracking (balances, sequence numbers), and settlement.
    *   Handles signing and verification of off-chain payment messages.
*   **Enhanced WalletProvider:**
    *   Supports multi-signature address creation and transaction signing.
    *   Manages keys for off-chain message signing.
*   **SPV Protocol Module:** Still needed for verifying the on-chain channel open and close transactions.
*   **BlockHeaderService & Transaction Broadcasting Service:** As in Approach 1, for on-chain transactions.

### 3.4. Pros

*   **Low-Cost Micro-payments:** Off-chain payments are virtually free and very fast, ideal for streaming or metered usage.
*   **Reduced On-Chain Load:** Only two on-chain transactions are typically needed per channel (open and close), regardless of the number of off-chain payments.
*   **Improved UX for Continuous Services:** Payments can happen in the background without interrupting service consumption.
*   **Flexibility:** Can support various pricing models (per-message, per-byte, per-time-unit).

### 3.5. Cons

*   **Increased Complexity:** Implementing and managing payment channels is significantly more complex than direct SPV payments. Requires careful state management and handling of potential disputes.
*   **Funds Lock-up:** Both parties need to lock up funds in the channel for its duration.
*   **Liveness Requirement (for interactive updates):** Both peers generally need to be online to exchange off-chain updates, though some designs allow for asynchronous updates.
*   **Onboarding Overhead:** Initial channel setup involves an on-chain transaction and negotiation.

### 3.6. Integration Points in `dart-libp2p-merkle-crdt`

*   **New Module `lib/src/payment_channel/`:**
    *   Houses the `Payment Channel Management Module` logic.
    *   Implements the libp2p stream handler for `/payment-channel/1.0.0`.
*   **Enhance `lib/src/spv/` (or ensure it's usable by payment channel module):** For on-chain channel transaction verification.
*   **Modify Service Protocols (e.g., `/crdt-dag-sync/1.0.0`):**
    *   Instead of (or in addition to) AccessTokens, these protocols might require or accept off-chain payment messages via the payment channel stream as proof of payment for specific data units.
    *   Alternatively, a service might require an active payment channel with sufficient funds before granting any access.
*   **Application Layer:**
    *   Orchestrates channel establishment based on service requirements.
    *   Defines policies for when and how off-chain payments are triggered by service consumption.

## 4. Hybrid Approaches and Considerations

*   **Credit-Based System (using Approach 1):** A user could make a larger SPV payment (Approach 1) to purchase "credit." This credit is then debited locally or via simpler off-chain messages (less complex than full payment channels) as services are consumed. This offers more granularity than pure time-based subscriptions without the full complexity of payment channels.
*   **Coexistence:**
    *   Approach 1 could be used for baseline subscription access (e.g., monthly fee).
    *   Approach 2 (Payment Channels) could be used for premium, high-usage features or for paying relay peers for fine-grained services on top of a basic subscription.
*   **Progressive Implementation:** It might be pragmatic to implement Approach 1 first, as it's conceptually simpler and aligns with existing documentation. Approach 2 can be considered a future enhancement for more advanced use cases.

## 5. Conclusion

Both proposed approaches offer viable paths to integrating Bitcoin payments into the `dart-libp2p-merkle-crdt` project, each with distinct trade-offs regarding complexity, payment granularity, and cost.

*   **SPV Payments with Access Tokens** provide a solid foundation for subscription-based services.
*   **Payment Channels** offer a more advanced solution for streaming and micropayment scenarios, enabling finer-grained monetization at the cost of increased implementation complexity.

The choice of approach, or a hybrid model, will depend on the specific monetization goals, the nature of the services being offered, and development resources. Careful consideration of the integration points and required components will be crucial for successful implementation.
