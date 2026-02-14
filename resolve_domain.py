import argparse
import asyncio
import dns.message
import dns.rcode
import dns.rdatatype
import httpx
import os
import random
import sys
import time

# Optimized DoH Server Pool
DOH_SERVER_LIST = [
    "https://cloudflare-dns.com/dns-query",
    "https://dns.adguard-dns.com/dns-query",
    "https://dns.controld.com/com-default",
    "https://dns.google/dns-query",
    "https://dns.quad9.net/dns-query",
    "https://doh.cleanbrowsing.org/doh/family-filter/",
    "https://doh.dns.sb/dns-query",
    "https://doh.gcore.com/dns-query" "https://doh.libredns.gr/dns-query",
    "https://doh.mullvad.net/dns-query",
    "https://doh.opendns.com/dns-query",
    "https://doh.powerdns.org",
]

# Tuning for GitHub Actions (2-core runners)
CONCURRENCY = 300
DNS_TIMEOUT = 5.0
RETRIES = 2


async def resolve_query(domain, qtype, session):
    """Attempt resolution across the server pool with retries."""
    q = dns.message.make_query(domain, qtype)
    query_data = q.to_wire()
    headers = {
        "Content-Type": "application/dns-message",
        "Accept": "application/dns-message",
        "User-Agent": "GitHubAction-DNS-Resolver/1.0",
    }

    # Shuffle servers for this specific domain request to spread load
    servers = random.sample(DOH_SERVER_LIST, min(len(DOH_SERVER_LIST), RETRIES + 1))

    for url in servers:
        try:
            resp = await session.post(
                url, content=query_data, headers=headers, timeout=DNS_TIMEOUT
            )
            if resp.status_code == 200:
                msg = dns.message.from_wire(resp.content)
                if msg.rcode() == dns.rcode.NOERROR:
                    return [
                        r.to_text()
                        for section in msg.answer
                        if section.rdtype
                        == (dns.rdatatype.A if qtype == "A" else dns.rdatatype.AAAA)
                        for r in section
                    ]
                elif msg.rcode() == dns.rcode.NXDOMAIN:
                    return []
        except Exception:
            continue
    return []


async def worker(domain, semaphore, session):
    """Processes a single domain."""
    async with semaphore:
        try:
            # Resolve A and AAAA concurrently
            res_a, res_aaaa = await asyncio.gather(
                resolve_query(domain, "A", session),
                resolve_query(domain, "AAAA", session),
            )
            all_res = res_a + res_aaaa
            return f"{domain} {' '.join(all_res)}" if all_res else None
        except Exception:
            return None


async def main_async(domain_list):
    # Performance-tuned HTTP client
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=CONCURRENCY)
    async with httpx.AsyncClient(http2=True, limits=limits, trust_env=False) as session:
        semaphore = asyncio.Semaphore(CONCURRENCY)

        # Batch processing to manage memory for very large lists
        total = len(domain_list)
        print(f"Starting resolution of {total} domains (Concurrency: {CONCURRENCY})")

        start_time = time.time()
        results = []
        tasks = [worker(d, semaphore, session) for d in domain_list]

        completed = 0
        for future in asyncio.as_completed(tasks):
            res = await future
            completed += 1
            if res:
                results.append(res)

            # Progress reporting optimized for GitHub Action logs
            if completed % 500 == 0 or completed == total:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                print(
                    f"[{completed/total:>7.2%}] {completed}/{total} | Resolved: {len(results)} | Rate: {rate:.1f} dom/s",
                    flush=True,
                )

        end_time = time.time()
        print(f"\nFinal Statistics:")
        print(f"  Total Domains:    {total}")
        print(f"  Successfully Resolved: {len(results)}")
        print(f"  Success Rate:     {len(results)/total:>7.2%}")
        print(f"  Total Duration:   {end_time - start_time:.2f}s")

        # Final Sort and Write
        results.sort()
        with open("resolve_result.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(results) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    args = parser.parse_args()

    if not os.path.exists("all_domain.txt"):
        print("Error: all_domain.txt not found.")
        return

    with open("all_domain.txt", "r", encoding="utf-8") as f:
        full_list = [
            line.strip() for line in f if line.strip() and " " not in line.strip()
        ]

    # Shuffle for consistent distribution across shards
    random.seed(42)
    random.shuffle(full_list)

    # Sharding Logic
    n = len(full_list)
    start = args.shard_index * (n // args.total_shards) + min(
        args.shard_index, n % args.total_shards
    )
    end = (args.shard_index + 1) * (n // args.total_shards) + min(
        args.shard_index + 1, n % args.total_shards
    )
    my_domains = full_list[start:end]

    print(
        f"Running Shard {args.shard_index + 1}/{args.total_shards} ({len(my_domains)} domains)"
    )

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if my_domains:
        asyncio.run(main_async(my_domains))


if __name__ == "__main__":
    main()
