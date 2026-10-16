#!/usr/bin/env python3
"""
BULK STRESS TEST
================
Creates a large number of orders as fast as possible and verifies
that the Notification service received all of them.
"""

import argparse
import asyncio
import time

import httpx


ORDER_URL = "http://localhost:8000"
NOTIFICATION_URL = "http://localhost:8001"


PRODUCTS = [
    {"name": "Laptop", "price": 999.99},
    {"name": "Phone", "price": 699.00},
    {"name": "Headphones", "price": 149.50},
    {"name": "Monitor", "price": 279.00},
    {"name": "Keyboard", "price": 89.99},
    {"name": "Mouse", "price": 39.99},
    {"name": "USB Hub", "price": 24.50},
    {"name": "Webcam", "price": 59.00},
]


async def create_order(client: httpx.AsyncClient, i: int) -> dict:
    product = PRODUCTS[i % len(PRODUCTS)]
    qty = (i % 3) + 1
    payload = {
        "user_email": f"user{i % 50}@example.com",
        "items": [{"name": product["name"], "qty": qty, "price": product["price"]}],
    }
    resp = await client.post(f"{ORDER_URL}/orders", json=payload, timeout=20.0)
    resp.raise_for_status()
    return resp.json()


async def get_count(client: httpx.AsyncClient, url: str, path: str) -> int:
    try:
        resp = await client.get(f"{url}{path}", timeout=8.0)
        if resp.status_code == 200:
            return len(resp.json())
    except Exception:
        pass
    return -1


async def service_up(client: httpx.AsyncClient, url: str) -> bool:
    try:
        r = await client.get(f"{url}/health", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


async def run_stress_test(total: int, concurrency: int):
    print("=" * 70)
    print(f"BULK STRESS TEST  |  orders={total}  concurrency={concurrency}")
    print("=" * 70)

    async with httpx.AsyncClient() as client:
        print("\n[1] Service status")
        order_ok = await service_up(client, ORDER_URL)
        notif_ok = await service_up(client, NOTIFICATION_URL)

        print(f"    Order Service        : {'✓ UP' if order_ok else '✗ DOWN'}")
        print(f"    Notification Service : {'✓ UP' if notif_ok else '✗ DOWN'}")

        if not order_ok:
            print("\nERROR: Order Service must be running.")
            return
        if not notif_ok:
            print("\nERROR: Notification Service must be running.")
            return

        print("\n[2] Baseline counts")
        orders_before = await get_count(client, ORDER_URL, "/orders")
        notifs_before = await get_count(client, NOTIFICATION_URL, "/notifications")

        print(f"    Orders        : {orders_before}")
        print(f"    Notifications : {notifs_before}")

        print(f"\n[3] Creating {total} orders (concurrency={concurrency})...")
        created = 0
        failed = 0
        start = time.perf_counter()

        for batch_start in range(0, total, concurrency):
            batch_end = min(batch_start + concurrency, total)
            tasks = [create_order(client, i) for i in range(batch_start, batch_end)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, Exception):
                    failed += 1
                else:
                    created += 1

            done = created + failed
            elapsed = time.perf_counter() - start
            rate = created / elapsed if elapsed > 0 else 0
            print(f"    Progress: {done}/{total}  |  ok={created} fail={failed}  |  {rate:.1f} orders/sec")

        total_time = time.perf_counter() - start
        print(f"\n    Finished producing in {total_time:.2f}s  ({created/total_time:.1f} orders/sec)")

        if created == 0:
            print("ERROR: No orders created.")
            return

        print("\n[4] Waiting for Notification service to catch up...")
        expected_notifs = notifs_before + created

        max_wait = 40
        poll = 1.5
        waited = 0.0
        final_notifs = notifs_before

        while waited < max_wait:
            final_notifs = await get_count(client, NOTIFICATION_URL, "/notifications")
            print(f"    Notifications: {final_notifs}/{expected_notifs}  (waited {waited:.0f}s)")

            if final_notifs >= expected_notifs:
                break

            await asyncio.sleep(poll)
            waited += poll

        print("\n[5] Final Results")
        print(f"    Orders created     : {created}  (failed: {failed})")
        got_n = final_notifs - notifs_before
        print(f"    Notifications got  : {got_n}/{created}")
        print(f"    Produce time       : {total_time:.2f}s")
        print(f"    Throughput         : {created/total_time:.1f} orders/sec")

        print("\n" + "=" * 70)
        if got_n >= created:
            print("✅ STRESS TEST PASSED")
        else:
            print("❌ STRESS TEST FAILED")
            print(f"   Notification missing: {created - got_n}")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Kafka bulk stress test")
    parser.add_argument("--count", type=int, default=50, help="Total orders to create (default: 50)")
    parser.add_argument("--concurrency", type=int, default=10, help="Parallel requests (default: 10)")
    args = parser.parse_args()

    asyncio.run(run_stress_test(args.count, args.concurrency))


if __name__ == "__main__":
    main()
