"""Realistic web-log generator.

Produces JSON-serialisable dicts that look like parsed Nginx/Apache access-log lines.
Built-in "stories" make the dashboard interesting:

  * daily-style wobble in the request rate
  * traffic SPIKES   (rate x3 for 15 s, every `spike_every` seconds)
  * ERROR BURSTS     (/checkout and /api/* start failing with 5xx for 20 s, every `error_every` seconds)
  * BOT traffic      (a few IPs hammer /login and /api/search at a very high rate)
"""
import math
import random
import time

PAGES = [
    ("/", 22), ("/products", 14), ("/products/laptops", 8), ("/products/phones", 8),
    ("/products/accessories", 5), ("/cart", 6), ("/checkout", 4), ("/search", 7),
    ("/blog", 5), ("/blog/big-data-trends", 3), ("/about", 2), ("/contact", 2),
    ("/api/products", 6), ("/api/search", 5), ("/api/cart", 4), ("/login", 4),
    ("/account", 3), ("/static/app.js", 6), ("/static/style.css", 6), ("/favicon.ico", 3),
]
COUNTRIES = [  # (country, weight, ip prefixes)
    ("US", 30, ["34.201", "52.14", "73.92", "98.17"]),
    ("IN", 22, ["49.36", "103.21", "106.51", "157.32"]),
    ("GB", 9, ["81.2", "86.14", "92.40"]),
    ("DE", 8, ["85.214", "91.64", "217.23"]),
    ("BR", 6, ["177.10", "189.45", "201.17"]),
    ("FR", 5, ["78.192", "90.5"]),
    ("JP", 5, ["126.12", "133.20"]),
    ("CA", 4, ["24.114", "99.250"]),
    ("AU", 3, ["1.128", "101.160"]),
    ("SG", 3, ["119.74", "175.41"]),
    ("RU", 3, ["95.24", "178.45"]),
    ("CN", 2, ["114.80", "223.104"]),
]
USER_AGENTS = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0", 38),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) Safari/605.1", 18),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_4) Mobile Safari/604.1", 22),
    ("Mozilla/5.0 (Linux; Android 14) Chrome/124.0 Mobile", 16),
    ("Mozilla/5.0 (X11; Linux x86_64) Firefox/125.0", 6),
]
BOT_UA = "python-requests/2.31.0"
POST_PAGES = {"/login", "/checkout", "/api/cart"}


def _weighted(items):
    population = [i[0] for i in items]
    weights = [i[1] for i in items]
    return population, weights


class TrafficGenerator:
    def __init__(self, base_rate=60, spike_every=90, error_every=150, seed=None, users=900):
        self.rng = random.Random(seed)
        self.base_rate = base_rate
        self.spike_every = spike_every
        self.error_every = error_every
        self.t0 = time.time()
        self._pages, self._page_w = _weighted(PAGES)
        self._uas, self._ua_w = _weighted(USER_AGENTS)
        self._countries = [c[0] for c in COUNTRIES]
        self._country_w = [c[1] for c in COUNTRIES]
        self._prefix = {c[0]: c[2] for c in COUNTRIES}
        # fixed pool of regular visitors: (ip, country, user-agent)
        self.users = []
        for _ in range(users):
            country = self.rng.choices(self._countries, self._country_w)[0]
            self.users.append((self._make_ip(country), country, self.rng.choices(self._uas, self._ua_w)[0]))
        self.bots = [
            (self._make_ip("RU"), "RU", BOT_UA),
            (self._make_ip("CN"), "CN", BOT_UA),
            (self._make_ip("BR"), "BR", BOT_UA),
        ]

    def _make_ip(self, country):
        prefix = self.rng.choice(self._prefix[country])
        parts = prefix.split(".")
        while len(parts) < 4:
            parts.append(str(self.rng.randint(1, 254)))
        return ".".join(parts)

    # ---- scenario state -------------------------------------------------
    def in_spike(self, t):
        return self.spike_every > 0 and t > 20 and (t % self.spike_every) < 15

    def in_error_burst(self, t):
        return self.error_every > 0 and t > 40 and (t % self.error_every) < 20

    def rate(self, t):
        wobble = 1 + 0.25 * math.sin(t / 25.0)
        r = self.base_rate * wobble
        if self.in_spike(t):
            r *= 3
        return r

    # ---- event creation -------------------------------------------------
    def _event(self, now_ms, t):
        bot = self.rng.random() < 0.06
        if bot:
            ip, country, ua = self.rng.choice(self.bots)
            url = self.rng.choice(["/login", "/api/search", "/login", "/api/products"])
        else:
            ip, country, ua = self.rng.choice(self.users)
            url = self.rng.choices(self._pages, self._page_w)[0]
        method = "POST" if (url in POST_PAGES and self.rng.random() < 0.7) else "GET"

        burst = self.in_error_burst(t) and (url.startswith("/checkout") or url.startswith("/api/"))
        r = self.rng.random()
        if burst and r < 0.55:
            status = self.rng.choice([500, 500, 502, 503, 504])
        elif bot and url == "/login" and r < 0.8:
            status = 401
        elif r < 0.885:
            status = 200
        elif r < 0.925:
            status = 304
        elif r < 0.95:
            status = 301
        elif r < 0.98:
            status = 404
        elif r < 0.99:
            status = 403
        else:
            status = self.rng.choice([500, 502, 503])

        base = self.rng.lognormvariate(4.6, 0.5)  # median ~100 ms
        if url.startswith("/api/"):
            base *= 1.6
        if url.startswith("/static") or url == "/favicon.ico":
            base *= 0.3
        if status >= 500:
            base *= 4
        if self.in_spike(t):
            base *= 1.4
        size = 0 if status in (304, 301) else int(self.rng.lognormvariate(8.4, 1.0))
        return {
            "ts": now_ms, "ip": ip, "method": method, "url": url, "status": status,
            "bytes": size, "rt_ms": int(base), "country": country, "ua": ua,
        }

    def events(self, dt, now=None):
        """Events for a time slice of `dt` seconds."""
        now = now if now is not None else time.time()
        t = now - self.t0
        lam = self.rate(t) * dt
        n = max(0, int(round(self.rng.gauss(lam, math.sqrt(max(lam, 1))))))
        return [self._event(int(now * 1000), t) for _ in range(n)]
