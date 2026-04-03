import asyncio
import httpx
import time
import pandas as pd

TEST_IDS = pd.read_csv('products-0-200000.csv')['id'].head(1000).tolist()
BASE_URL = "https://api.tiki.vn/product-detail/api/v1/products/"


async def test_limit(limit):
    semaphore = asyncio.Semaphore(limit)
    success = 0
    errors = 0
    start_time = time.perf_counter()

    async def fetch(client, pid):
        nonlocal success, errors
        async with semaphore:
            try:
                r = await client.get(f"{BASE_URL}{pid}", timeout=5.0)
                if r.status_code == 200:
                    success += 1
                else:
                    errors += 1
            except:
                errors += 1

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=limit)) as client:
        # Test 200 requests for this specific limit
        tasks = [fetch(client, pid) for pid in TEST_IDS[:200]]
        await asyncio.gather(*tasks)

    end_time = time.perf_counter()
    rate = success / (end_time - start_time)
    error_percent = (errors / 200) * 100
    return rate, error_percent


async def run_stress_test():
    print(f"{'Limit':<10} | {'Req/s':<10} | {'Error %':<10}")
    print("-" * 35)
    for limit in range(10,101,5):
        rate, err = await test_limit(limit)
        print(f"{limit:<10} | {rate:<10.2f} | {err:<10.1f}%")
        if err > 50:
            print(f"--> Limit {limit} is too high (Tiki is blocking)!")
            break
        await asyncio.sleep(2)  # Brief cooldown between tests


if __name__ == "__main__":
    asyncio.run(run_stress_test())