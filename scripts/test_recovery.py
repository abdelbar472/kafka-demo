#!/usr/bin/env python3
"""
RECOVERY TEST
=============
Goal: Prove that if a consumer is DOWN when messages are produced,
      it will still process them AFTER it comes back online.
"""

import argparse
import asyncio
import time

import httpx


ORDER_URL = "http://localhost:8000"
NOTIFICATION_URL = "http://localhost:8001"


async def is_up(client: httpx.AsyncClient, url: str) -> bool:
    try:
        r = await client.get(f"{url}/health", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


async def create_order(client: httpx.AsyncClient, email: str) -> dict:
    payload = {
        "user_email": email,
        "items": [{"name": "Recovery Test Item", "qty": 1, "price": 49.99}],
    }
    resp = await client.post(f"{ORDER_URL}/orders", json=payload, timeout=15.0)
    resp.raise_for_status()
    return resp.json()


async def wait_until_up(client: httpx.AsyncClient, url: str, name: str, timeout: int = 60):
    print(f"\n⏳ Waiting for {name} to come back online...")
    start = time.time()
    while time.time() - start < timeout:
        if await is_up(client, url):
            print(f"   ✓ {name} is UP")
            return True
        await asyncio.sleep(1.5)
        print("   ... still down")
    print(f"   ✗ Timeout waiting for {name}")
    return False


async def run_recovery_test(count: int):
    print("=" * 65)
    print("RECOVERY TEST: Consumer down → Produce → Consumer back → Catch-up")
    print("=" * 65)

    async with httpx.AsyncClient() as client:
        print("\n[1] Checking Order Service...")
        if not await is_up(client, ORDER_URL):
            print("ERROR: Order Service is not running on port 8000")
            return
        print("   ✓ Order Service is up")

        print("\n[2] Checking Notification Service status...")
        if await is_up(client, NOTIFICATION_URL):
            print("   ⚠ Notification Service is currently RUNNING.")
            print("   → Please STOP it now (Ctrl+C in its terminal)")
            print("   → Then press ENTER here to continue...")
            input()
            for _ in range(20):
                if not await is_up(client, NOTIFICATION_URL):
                    break
                await asyncio.sleep(0.5)
            if await is_up(client, NOTIFICATION_URL):
                print("ERROR: Notification Service is still up. Aborting.")
                return
        print("   ✓ Notification Service is DOWN (good)")

        print("\n[3] Creating orders WHILE Notification Service is DOWN...")
        created_ids = []
        for i in range(count):
            email = f"recovery+{i}@example.com"
            order = await create_order(client, email)
            created_ids.append(order["id"])
            print(f"   ✓ Order {i+1}/{count} created → {order['id'][:8]}...")

        print(f"\n   → {count} messages are now sitting in Kafka topic 'order.created'")
        print("   → Notification Service has NOT consumed them yet")

        print("\n[4] Now START the Notification Service again:")
        print("    cd notification-service")
        print("    uvicorn main:app --port 8001 --reload")
        print("\n   Press ENTER after it has started...")
        input()

        if not await wait_until_up(client, NOTIFICATION_URL, "Notification Service"):
            return

        print("\n[5] Waiting for catch-up (Kafka consumer will process missed messages)...")
        await asyncio.sleep(4)

        print("\n[6] Checking if missed messages were processed...")
        max_wait = 20
        found = 0
        for sec in range(max_wait):
            try:
                notifs = await client.get(f"{NOTIFICATION_URL}/notifications", timeout=5.0)
                notifs = notifs.json()
                notif_order_ids = {n["order_id"] for n in notifs}
                found = sum(1 for oid in created_ids if oid in notif_order_ids)
                print(f"   Processed: {found}/{count}")
                if found >= count:
                    break
            except Exception as e:
                print(f"   Error reading notifications: {e}")
            await asyncio.sleep(1)

        print("\n" + "=" * 65)
        if found >= count:
            print("✅ RECOVERY TEST PASSED")
            print(f"   All {count} messages produced while the service was DOWN")
            print("   were successfully processed after it came back online.")
            print("\n   This is one of the main strengths of Kafka:")
            print("   → Messages are stored in the topic")
            print("   → Consumers can catch up later using their offsets")
        else:
            print("❌ RECOVERY TEST FAILED")
            print(f"   Only {found}/{count} messages were processed.")
            print("   Check Notification Service logs for errors.")
        print("=" * 65)


def main():
    parser = argparse.ArgumentParser(description="Test Kafka consumer recovery")
    parser.add_argument("--count", type=int, default=5, help="How many orders to create while down")
    args = parser.parse_args()
    asyncio.run(run_recovery_test(args.count))


if __name__ == "__main__":
    main()
