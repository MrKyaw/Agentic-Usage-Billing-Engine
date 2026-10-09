"""Locust load test — PRD §6: ≥500 ev/s sustained, burst 1000. p99 ingest <200ms."""
from locust import HttpUser, between, task
import random
import uuid


class UsageUser(HttpUser):
    wait_time = between(0.001, 0.01)

    @task(8)
    def single(self):
        self.client.post("/v1/usage-events", headers={"X-API-Key": "dev-key-123"}, json={
            "event_id": str(uuid.uuid4()), "customer_id": random.choice(["acme", "globex", "initech"]),
            "product_id": "design-suite", "metric": random.choice(["cloud_credits", "render_minutes"]),
            "quantity": random.randint(1, 20), "timestamp": "2026-10-09T10:00:00Z"})

    @task(1)
    def batch(self):
        evs = [{"event_id": str(uuid.uuid4()), "customer_id": "acme", "product_id": "design-suite",
                "metric": "cloud_credits", "quantity": 1,
                "timestamp": "2026-10-09T10:00:00Z"} for _ in range(20)]
        self.client.post("/v1/usage-events:batch", headers={"X-API-Key": "dev-key-123"},
                         json={"events": evs})

    @task(1)
    def read_bill(self):
        self.client.get("/v1/customers/acme/bill?period=2026-10",
                        headers={"X-API-Key": "dev-key-123"})
