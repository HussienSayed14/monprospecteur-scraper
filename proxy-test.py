import requests
r = requests.get(
    "https://ipv4.webshare.io/",
    proxies={
        "http":  "http://prbspgoz:x8f4iu7gqoet@82.23.96.252:7478",
        "https": "http://prbspgoz:x8f4iu7gqoet@82.23.96.252:7478"
    }
)
print(r.text)  # should print a Canadian IP address