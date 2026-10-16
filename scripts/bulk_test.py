#!/usr/bin/env python3
"""
Bulk test script for Order → Kafka → Notification pipeline.
"""

import argparse
import asyncio
import time
from typing import List

import httpx


ORDER_URL = "http://localhost:8000"
NOTIFICATION_URL = "http://localhost:8001"


SAMPLE_ITEMS = [
    {"name": "Laptop", "qty": 1, "price": 999.99},
    {"name": "Mouse", "qty": 2, "price": 29.99},
    {"name": "Keyboard", "qty": 1, "price": 79.50},
    {"name": "Monitor", "qty": 1, "price": 299.00},
    {"name": "USB Cable", "qty": 3, "price": 9.99},
]


async def create_order(client: httpx.AsyncClient, email: str, items: List[dict]) -> dict:
    payload = {"user_email": email, "items": items}
    resp = await client.post(f"{ORDER_URL}/orders", json=payload, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def list_orders(client: httpx.AsyncClient) -> List[dict]:
    resp = await client.get(f"{ORDER_URL}/orders", timeout=10.0)
    resp.raise_for_status()
    return resp.json()


async def list_notifications(client: httpx.AsyncClient) -> List[dict]:
    resp = await client.get(f"{NOTIFICATION_URL}/notifications", timeout=10.0)
    resp.raise_for_status()
    return resp.json()


async def health_check(client: httpx.AsyncClient) -> bool:
    try:
        o = await client.get(f"{ORDER_URL}/health", timeout=5.0)
        n = await client.get(f"{NOTIFICATION_URL}/health", timeout=5.0)
        return o.status_code == 200 and n.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False


async def run_bulk_test(count: int, base_email: str):
    print("=" * 60)
    print("BULK TEST: Order → Kafka → Notification")
    print("=" * 60)

    async with httpx.AsyncClient() as client:
        print("\n[1] Checking services health...")
        if not await health_check(client):
            print("ERROR: One or both services are not reachable.")
            print(f"  Order service expected at {ORDER_URL}")
            print(f"  Notification service expected at {NOTIFICATION_URL}")
            return
        print("    ✓ Both services are healthy")

        print("\n[2] Recording baseline counts...")
        orders_before = await list_orders(client)
        notifs_before = await list_notifications(client)
        print(f"    Orders before      : {len(orders_before)}")
        print(f"    Notifications before: {len(notifs_before)}")

        print(f"\n[3] Creating {count} orders...")
        created_orders = []
        start = time.perf_counter()

        tasks = []
        for i in range(count):
            email = f"{base_email.split('@')[0]}+{i}@{base_email.split('@')[1]}"
            items = [SAMPLE_ITEMS[i % len(SAMPLE_ITEMS)]]
            if i % 3 == 0:
                items = SAMPLE_ITEMS[:2]
            tasks.append(create_order(client, email, items))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        success = 0
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                print(f"    ✗ Order {i} failed: {res}")
            else:
                success += 1
                created_orders.append(res)
                print(f"    ✓ Order {i+1}/{count} → {res['id'][:8]}... total=${res['total']:.2f}")

        elapsed = time.perf_counter() - start
        print(f"\n    Created {success}/{count} orders in {elapsed:.2f}s")

        if success == 0:
            print("ERROR: No orders were created. Aborting.")
            return

        print("\n[4] Waiting for notifications to be processed...")
        await asyncio.sleep(3)

        expected = len(notifs_before) + success
        max_wait = 15
        waited = 0.0
        final_notifs = []

        while waited < max_wait:
            final_notifs = await list_notifications(client)
            current = len(final_notifs)
            print(f"    Notifications: {current} (expecting ≥ {expected})")
            if current >= expected:
                break
            await asyncio.sleep(1)
            waited += 1

        print("\n[5] Verification")
        orders_after = await list_orders(client)
        notifs_after = final_notifs

        new_orders = len(orders_after) - len(orders_before)
        new_notifs = len(notifs_after) - len(notifs_before)

        print(f"    New orders created     : {new_orders}")
        print(f"    New notifications      : {new_notifs}")

        order_ids = {o["id"] for o in created_orders}
        notif_order_ids = {n["order_id"] for n in notifs_after}

        matched = order_ids & notif_order_ids
        missing = order_ids - notif_order_ids

        print(f"    Matched notifications  : {len(matched)}/{len(order_ids)}")
        if missing:
            print(f"    Missing for orders     : {list(missing)[:5]}{'...' if len(missing) > 5 else ''}")

        print("\n[6] Sample notifications:")
        for n in notifs_after[:3]:
            print(f"    • {n['message']}")
            print(f"      order_id={n['order_id'][:8]}... email={n['user_email']}")

        print("\n" + "=" * 60)
        if new_notifs >= success and len(missing) == 0:
            print("✅ BULK TEST PASSED")
        else:
            print("❌ BULK TEST FAILED")
            print("   Possible causes: Kafka not running, consumer lag, or service error.")
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Bulk test Order → Notification pipeline")
    parser.add_argument("--count", type=int, default=10, help="Number of orders (default: 10)")
    parser.add_argument("--base-email", type=str, default="tester@example.com")
    args = parser.parse_args()

    asyncio.run(run_bulk_test(args.count, args.base_email))


if __name__ == "__main__":
    main()
