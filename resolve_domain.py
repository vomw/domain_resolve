import argparse
import asyncio
import dns.asyncquery
import dns.message
import dns.rcode
import httpx
import os
import random
import sys
import time

# DoH Server Pool for Load Balancing and MITM protection
DOH_SERVER_LIST = [
    "https://1.0.0.1/dns-query",
    "https://1.0.0.2/dns-query",
    "https://1.0.0.3/dns-query",
    "https://1.1.1.1/dns-query",
    "https://1.1.1.2/dns-query",
    "https://1.1.1.3/dns-query",
    "https://146.112.41.2/dns-query",
    "https://146.112.41.3/dns-query",
    "https://146.112.41.4/dns-query",
    "https://146.112.41.5/dns-query",
    "https://149.112.112.10/dns-query",
    "https://149.112.112.11/dns-query",
    "https://149.112.112.112/dns-query",
    "https://149.112.112.12/dns-query",
    "https://149.112.112.13/dns-query",
    "https://149.112.112.9/dns-query",
    "https://204.194.232.200/dns-query",
    "https://208.67.220.123/dns-query",
    "https://208.67.220.2/dns-query",
    "https://208.67.220.220/dns-query",
    "https://208.67.222.123/dns-query",
    "https://208.67.222.2/dns-query",
    "https://208.67.222.222/dns-query",
    "https://8.8.4.4/dns-query",
    "https://8.8.8.8/dns-query",
    "https://9.9.9.10/dns-query",
    "https://9.9.9.11/dns-query",
    "https://9.9.9.12/dns-query",
    "https://9.9.9.13/dns-query",
    "https://9.9.9.9/dns-query",
]

DNS_TIMEOUT = 5.0  # Increased for DoH overhead
RETRIES = 2
CONCURRENCY = 150  # Slightly lower for DoH to be more polite/stable


async def resolve_query_with_retry(domain, qtype, session):
    """Perform a DNS-over-HTTPS query with retries across different providers."""
    # Use a copy to avoid modifying the global list
    servers = DOH_SERVER_LIST.copy()
    random.shuffle(servers)

    for i in range(min(RETRIES + 1, len(servers))):
        url = servers[i]
        q = dns.message.make_query(domain, qtype)
        try:
            # Authenticated and Encrypted DNS Query
            response = await dns.asyncquery.https(
                q, url, timeout=DNS_TIMEOUT, session=session
            )

            if response.rcode() == dns.rcode.NOERROR:
                results = []
                for answer in response.answer:
                    # Filter for correct type (A or AAAA)
                    if answer.rdtype == (
                        dns.rdatatype.A if qtype == "A" else dns.rdatatype.AAAA
                    ):
                        results.extend([r.to_text() for r in answer])
                return results
            elif response.rcode() == dns.rcode.NXDOMAIN:
                return []  # Domain definitely doesn't exist
        except Exception:
            # On error (timeout, rate limit, etc), retry with next server
            continue
    return []


async def worker(domain, semaphore, session):
    """Async worker for a single domain using DoH."""
    try:
        async with semaphore:
            # Resolve A and AAAA in parallel for this domain
            res_a, res_aaaa = await asyncio.gather(
                resolve_query_with_retry(domain, "A", session),
                resolve_query_with_retry(domain, "AAAA", session),
            )

            all_res = res_a + res_aaaa
            if not all_res:
                return None

            return f"{domain} {' '.join(all_res)}"
    except Exception as e:
        print(f"\nError processing {domain}: {e}", file=sys.stderr)
        return None


def get_sharded_list(full_list, shard_index, total_shards):
    """Split the list into shards."""
    if total_shards <= 1:
        return full_list
    total_items = len(full_list)
    chunk_size = total_items // total_shards
    remainder = total_items % total_shards
    start = shard_index * chunk_size + min(shard_index, remainder)
    end = start + chunk_size + (1 if shard_index < remainder else 0)
    return full_list[start:end]


async def main_async(domain_list):
    # Use a single httpx client for connection pooling across all requests
    async with httpx.AsyncClient(http2=True, verify=True) as session:
        semaphore = asyncio.Semaphore(CONCURRENCY)

        # Create tasks
        tasks = [worker(domain, semaphore, session) for domain in domain_list]

        total = len(domain_list)
        print(f"Starting async DoH resolution of {total} domains...", flush=True)
        print(f"Using {len(DOH_SERVER_LIST)} providers for load balancing.")

        start_time = time.time()
        results = []
        completed = 0

        for future in asyncio.as_completed(tasks):
            res = await future
            completed += 1

            if res:
                results.append(res)

            if completed % 50 == 0 or completed == total:
                percent = (completed / total) * 100
                domain_name = res.split()[0] if res else "..."
                print(
                    f"[{percent:6.2f}%] {completed}/{total} - {domain_name}", flush=True
                )

        end_time = time.time()
        print(f"Finished in {end_time - start_time:.2f} seconds.", flush=True)
        print(f"Successfully resolved {len(results)}/{total} domains.")

        print("Sorting results...")
        results.sort()

        with open("resolve_result.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(results) + "\n")


def read_domain_list_from_file():
    if not os.path.exists("all_domain.txt"):
        return []
    with open("all_domain.txt", "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and " " not in line.strip()]


def main():
    parser = argparse.ArgumentParser(description="Async DoH Resolver")
    parser.add_argument(
        "--shard-index", type=int, default=0, help="Index of the current shard"
    )
    parser.add_argument(
        "--total-shards", type=int, default=1, help="Total number of shards"
    )
    args = parser.parse_args()

    domain_list = read_domain_list_from_file()
    if not domain_list:
        print("No domains found in all_domain.txt")
        return

    # Shuffle with fixed seed
    random.seed(42)
    random.shuffle(domain_list)

    my_domains = get_sharded_list(domain_list, args.shard_index, args.total_shards)
    print(
        f"Shard {args.shard_index + 1}/{args.total_shards}: Processing {len(my_domains)} domains"
    )

    if not my_domains:
        return

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main_async(my_domains))
    except KeyboardInterrupt:
        print("Interrupted.")


if __name__ == "__main__":
    main()
