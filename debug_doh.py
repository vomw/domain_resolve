import asyncio
import httpx
import dns.message
import dns.rdatatype

async def test_doh_manual():
    url = "https://dns.google/dns-query"
    domain = "google.com"
    
    print(f"Testing manual DoH POST with {url} for {domain}...")
    
    q = dns.message.make_query(domain, "A")
    query_data = q.to_wire()
    headers = {
        "Content-Type": "application/dns-message",
        "Accept": "application/dns-message",
    }
    
    async with httpx.AsyncClient(http2=True) as client:
        try:
            resp = await client.post(url, content=query_data, headers=headers)
            print(f"HTTP Status: {resp.status_code}")
            if resp.status_code == 200:
                answer = dns.message.from_wire(resp.content)
                print(f"DNS RCODE: {dns.rcode.to_text(answer.rcode())}")
                for rrset in answer.answer:
                    print(f"Answer: {rrset}")
            else:
                print(f"Error Body: {resp.text}")
        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    asyncio.run(test_doh_manual())
