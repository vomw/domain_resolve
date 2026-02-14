import argparse
import asyncio
import os
import random
import sys
import time
import dns.asyncresolver
import dns.resolver
import dns.exception

# Configuration
DNS_SERVER_LIST = [
    "138.199.149.249:53",
    "149.112.112.10:53",
    "149.112.112.11:53",
    "149.112.112.112:53",
    "149.112.112.12:53",
    "149.112.112.9:53",
    "176.9.1.117:53",
    "176.9.93.198:53",
    "185.228.168.9:53",
    "185.228.169.9:53",
    "208.67.220.220:443",
    "208.67.220.222:443",
    "208.67.222.220:443",
    "208.67.222.222:443",
    "49.12.222.213:53",
    "49.12.223.2:53",
    "49.12.43.208:53",
    "49.12.67.122:53",
    "78.47.71.194:53",
    "8.8.4.4:53",
    "8.8.8.8:53",
    "88.198.122.154:53",
    "9.9.9.10:53",
    "9.9.9.11:53",
    "9.9.9.12:53",
    "9.9.9.9:53",
    "91.99.154.175:53",
]

DNS_TIMEOUT = 2.0
RETRIES = 2
CONCURRENCY = 200


async def resolve_query_with_retry(domain, qtype):
    """Perform a DNS query with retries using different nameservers."""
    for _ in range(RETRIES + 1):
        dns_server = random.choice(DNS_SERVER_LIST)
        ip, port = dns_server.rsplit(":", 1)
        port = int(port)

        resolver = dns.asyncresolver.Resolver(configure=False)
        resolver.nameservers = [ip]
        resolver.port = port
        resolver.timeout = DNS_TIMEOUT
        resolver.lifetime = DNS_TIMEOUT

        try:
            answers = await resolver.resolve(domain, qtype)
            return [r.to_text() for r in answers]
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            return []
        except (dns.resolver.Timeout, dns.exception.DNSException):
            continue
        except Exception:
            continue
    return []


async def worker(domain, semaphore):
    """Async worker for a single domain. Returns result string only if resolution succeeds."""
    async with semaphore:
        res_a = await resolve_query_with_retry(domain, "A")
        res_aaaa = await resolve_query_with_retry(domain, "AAAA")

        all_res = res_a + res_aaaa
        if not all_res:
            return None

        return f"{domain} {" ".join(all_res)}"


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
    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [worker(domain, semaphore) for domain in domain_list]

    total = len(domain_list)
    print(f"Starting async resolution of {total} domains...", flush=True)

    start_time = time.time()
    results = []

    completed = 0
    for future in asyncio.as_completed(tasks):
        res = await future
        completed += 1

        if res:
            results.append(res)

        if completed % 100 == 0 or completed == total:
            percent = (completed / total) * 100
            current_domain = res.split()[0] if res else "..."
            print(
                f"[{percent:6.2f}%] {completed}/{total} - {current_domain}", flush=True
            )

    end_time = time.time()
    print(f"Finished in {end_time - start_time:.2f} seconds.", flush=True)
    print(f"Successfully resolved {len(results)}/{total} domains.")

    # Sort results alphabetically
    print("Sorting results...")
    results.sort()

    # Write results
    with open("resolve_result.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(results) + "\n")


def read_domain_list_from_file():
    if not os.path.exists("all_domain.txt"):
        return []
    with open("all_domain.txt", "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and " " not in line.strip()]


def main():
    parser = argparse.ArgumentParser(description="Async DNS Resolver")
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="Index of the current shard (0-based)",
    )
    parser.add_argument(
        "--total-shards", type=int, default=1, help="Total number of shards"
    )
    args = parser.parse_args()

    domain_list = read_domain_list_from_file()

    if not domain_list:
        print("No domains found in all_domain.txt")
        return

    # 1. Randomly shuffle the domain list before processing
    # Using a fixed seed ensures consistent sharding across multiple runners
    random.seed(42)
    random.shuffle(domain_list)

    # Apply sharding
    my_domains = get_sharded_list(domain_list, args.shard_index, args.total_shards)
    print(
        f"Shard {args.shard_index + 1}/{args.total_shards}: Processing {len(my_domains)} domains"
    )

    if not my_domains:
        print("No domains in this shard.")
        return

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main_async(my_domains))
    except KeyboardInterrupt:
        print("Interrupted.")


if __name__ == "__main__":
    main()
