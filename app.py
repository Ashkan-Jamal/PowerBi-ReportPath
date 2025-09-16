import requests

BASE_DOMAIN = "https://omantracking2.com"

def check_render(app_id, report_id, render_id, token):
    url = f"{BASE_DOMAIN}/comGpsGate/api.v.1/applications/{app_id}/reports/{report_id}/renderings/{render_id}"
    headers = {"Authorization": token, "Accept": "application/json"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        return r.status_code, r.text
    except Exception as e:
        return "error", str(e)

# Example usage
status, content = check_render("6", "25", "118545", "v2:MDAwMDAyOTA3MzpkNWUzOTg1MjRjMTVkMjQ5MGMzMg==")
print(status, content)
