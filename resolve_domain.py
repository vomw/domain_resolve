import msvcrt
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import queue
import dns.resolver

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

MAX_THREADS = 200
DNS_TIMEOUT = 2.0
RETRIES = 2  # Total 3 attempts


class ProgressTracker:
    """Thread-safe progress tracking and console output."""

    def __init__(self, total):
        self.total = total
        self.count = 0
        self.lock = threading.Lock()

    def update(self, domain, results):
        with self.lock:
            self.count += 1
            if self.count % 10 == 0 or self.count == self.total:
                percent = (self.count / self.total) * 100
                res_str = " ".join(results[:2]) + ("..." if len(results) > 2 else "")
                line = (
                    f"\r [{percent:6.3f}%] {self.count}/{self.total} {domain} {res_str}"
                )
                # Pad to clear previous long lines
                sys.stdout.write(line.ljust(110)[:110])
                sys.stdout.flush()


def resolve_query_with_retry(domain, qtype):
    """Perform a DNS query with retries using different nameservers."""
    for _ in range(RETRIES + 1):
        dns_server = random.choice(DNS_SERVER_LIST)
        ip, port = dns_server.rsplit(":", 1)
        port = int(port)

        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = [ip]
        resolver.port = port
        resolver.timeout = DNS_TIMEOUT
        resolver.lifetime = DNS_TIMEOUT
        try:
            answers = resolver.resolve(domain, qtype)
            return [r.to_text() for r in answers]
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            return []  # No point in retrying if it definitely doesn't exist
        except (dns.resolver.Timeout, dns.exception.DNSException):
            continue  # Retry with a different server
    return []


def worker(domain, tracker, result_queue):
    """Worker task for a single domain."""
    res_a = resolve_query_with_retry(domain, "A")
    res_aaaa = resolve_query_with_retry(domain, "AAAA")

    all_res = res_a + res_aaaa
    result_queue.put(f"{domain} {" ".join(all_res)}")
    tracker.update(domain, all_res)


def file_writer(result_queue, stop_event):
    """Background thread to write results to file efficiently."""
    with open("resolve_result.txt", "a", encoding="utf-8") as f:
        while not stop_event.is_set() or not result_queue.empty():
            try:
                line = result_queue.get(timeout=0.1)
                f.write(line.strip() + "\n")
                result_queue.task_done()
            except queue.Empty:
                continue


def read_domain_list_from_file():
    """Read domains from input file."""
    if not os.path.exists("all_domain.txt"):
        return []
    with open("all_domain.txt", "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and " " not in line.strip()]


def get_key_listener():
    """Thread function to listen for 'q' to exit."""
    while True:
        if msvcrt.kbhit():
            key = msvcrt.getch()
            if key in (b"\x00", b"\xe0"):
                msvcrt.getch()
                continue
            if key in (b"q", b"Q"):
                print("\nExiting...")
                os._exit(1)
        time.sleep(0.5)


def main():
    domain_list = read_domain_list_from_file()
    if not domain_list:
        print("No domains found in all_domain.txt")
        return

    total = len(domain_list)
    tracker = ProgressTracker(total)
    result_queue = queue.Queue()
    stop_event = threading.Event()

    # Start writer thread
    writer_thread = threading.Thread(
        target=file_writer, args=(result_queue, stop_event), daemon=True
    )
    writer_thread.start()

    # Start key listener
    threading.Thread(target=get_key_listener, daemon=True).start()

    print(f"Starting resolution of {total} domains with {MAX_THREADS} threads...")
    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # map ensures all domains are processed
            executor.map(lambda d: worker(d, tracker, result_queue), domain_list)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        os._exit(1)

    # Signal writer to stop and wait for it
    stop_event.set()
    writer_thread.join()

    end_time = time.time()
    print(f"\nFinished in {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
