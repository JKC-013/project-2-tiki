import asyncio
import httpx
import pandas as pd
import json
import os
from tqdm import tqdm
import uvloop

# UBUNTU OPTIMIZATION: Faster event loop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# --- USE YOUR TESTED LIMIT HERE ---
CONCURRENT_REQUESTS = 45  # Change this based on your test result
BATCH_SIZE = 1000
# ----------------------------------

CSV_FILE = 'products-0-200000.csv'
OUTPUT_DIR = "crawled_data"
ERROR_FILE = "ErrorID.csv"
CHECKPOINT_FILE = "checkpoint.json"

state = {"success": 0, "errors": 0}


async def fetch_worker(client, product_id, semaphore, pbar):
    async with semaphore:
        try:
            headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64)"}
            resp = await client.get(f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}", timeout=5.0)

            if resp.status_code == 200:
                state["success"] += 1
                data = resp.json()
                return {
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "price": data.get("price"),
                    "description": data.get("description"),  # Clean this later in Jupyter
                    "images": [img.get("base_url") for img in data.get("images", [])]
                }
            else:
                raise Exception(f"HTTP {resp.status_code}")
        except Exception as e:
            state["errors"] += 1
            with open(ERROR_FILE, 'a') as f:
                f.write(f"{product_id},{str(e)}\n")
            return None
        finally:
            pbar.update(1)


async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.read_csv(CSV_FILE)
    p_ids = df['id'].tolist()

    start_idx = 0
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            start_idx = json.load(f).get("last_index", 0)

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    pbar = tqdm(total=len(p_ids), initial=start_idx, desc="CRAWLING")

    # TCP Keep-Alive optimization
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=CONCURRENT_REQUESTS)

    async with httpx.AsyncClient(limits=limits, verify=False) as client:
        for i in range(start_idx, len(p_ids), BATCH_SIZE):
            batch = p_ids[i: i + BATCH_SIZE]
            tasks = [fetch_worker(client, pid, semaphore, pbar) for pid in batch]

            results = await asyncio.gather(*tasks)
            valid_data = [r for r in results if r is not None]

            # Fast Save
            with open(f"{OUTPUT_DIR}/batch_{i // BATCH_SIZE + 1}.json", 'w', encoding='utf-8') as f:
                json.dump(valid_data, f, ensure_ascii=False)

            # Checkpoint
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump({"last_index": i + BATCH_SIZE}, f)

            pbar.set_postfix({"Success": state["success"], "Error": state["errors"]})


if __name__ == "__main__":
    asyncio.run(main())