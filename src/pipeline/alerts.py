import json
import urllib.error
import urllib.request


def send_webhook_alert(webhook_url: str, payload: dict[str, str]) -> str:
    if not webhook_url:
        return "not_configured"

    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status_code = getattr(response, "status", 200)
            return "sent" if 200 <= status_code < 300 else f"http_{status_code}"
    except urllib.error.HTTPError as exc:
        return f"http_{exc.code}"
    except urllib.error.URLError:
        return "network_error"
