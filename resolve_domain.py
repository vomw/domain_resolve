# import msvcrt
import os
import random
import subprocess
import sys
# import threading
import time


def dig_query(qname, qtype):
    dns_server = random.choice(
        [
            "208.67.220.220",
            "208.67.220.222",
            "208.67.222.220",
            "208.67.222.222",
        ]
    )
    command = f" dig +short -t{qtype} {qname} @{dns_server} -p443 "
    try:
        resp = subprocess.check_output(command, shell=True).decode("utf-8").strip()
    except subprocess.CalledProcessError as e:
        print(f"{type(e).__name__} {command} {str(e).strip()} ")
        print(e.stdout.decode("utf-8").strip())
        return dig_query(qname, qtype)
    return resp.split()


def read_domain_list_from_file():
    with open("all_domain.txt", "r", encoding="utf-8") as f:
        domain_list = f.readlines()
    return [i.strip() for i in domain_list if i.strip() if not " " in i.strip()]


def write_result_to_file(result):
    with open("resolve_result.txt", "a", encoding="utf-8") as f:
        f.write(result + "\n")


def main():
    domain_list = read_domain_list_from_file()
    # random.shuffle(domain_list)
    domain_list_count = len(domain_list)
    for i, domain in enumerate(domain_list, start=1):
        print(f" [{i / domain_list_count:.3%}] {domain} ", end="")
        sys.stdout.flush()
        resp_a = dig_query(domain, "A")
        resp_aaaa = dig_query(domain, "AAAA")
        resp = " ".join(resp_a + resp_aaaa)
        write_result_to_file(domain + " " + resp)
        print(resp)


# def get_key_listener():
#     """Thread function to listen for key presses."""
#     while True:
#         if msvcrt.kbhit():
#             key = msvcrt.getch()
#             if key in (b"\x00", b"\xe0"):
#                 msvcrt.getch()
#                 continue
#             if key in (b"q", b"Q"):
#                 print("\nExiting...")
#                 os._exit(1)
#         time.sleep(1)


if __name__ == "__main__":
    # key_thread = threading.Thread(target=get_key_listener, daemon=True)
    # key_thread.start()
    main()
