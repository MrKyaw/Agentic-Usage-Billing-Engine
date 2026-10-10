import json
import sys

import pytest
from conftest import make_event

from billing_engine import agent_demo
from billing_engine.ingest import ingest_one
from billing_engine.processor import drain

pytestmark = pytest.mark.req("FR7")


def _seed(store):
    ingest_one(store, make_event("evt-demo-001", quantity=2))
    ingest_one(store, make_event("r-1", customer_id="acme", metric="render_minutes",
                                 quantity=1100))
    drain(store)


def test_each_question_routes_to_the_right_tool(default_store):
    _seed(default_store)
    qs = [q.format(c="acme", p="2026-10", e="evt-demo-001") for q in agent_demo.QUESTIONS]
    usage, owe, recon, explain, most = (agent_demo.answer(q, "acme", "2026-10", "evt-demo-001")
                                        for q in qs)
    assert {line["metric"] for line in json.loads(usage)["lines"]} == {
        "cloud_credits", "render_minutes"}
    assert owe.startswith("acme owes $50.2 for 2026-10 (status=open)")
    assert recon.startswith("Reconciliation: match=True checked=2")
    ex = json.loads(explain)
    assert ex["found"] and ex["event"]["event_id"] == "evt-demo-001"
    assert "'metric': 'render_minutes'" in most  # highest-amount line


def test_explain_defaults_and_missing_event(default_store):
    _seed(default_store)
    assert json.loads(agent_demo.answer("explain event", "acme", "2026-10"))["found"]
    missing = json.loads(agent_demo.answer("explain event", "acme", "2026-10", "nope"))
    assert missing == {"found": False, "event_id": "nope", "customer_id": "acme"}


def test_empty_customer_bill_has_no_top_line(default_store):
    assert "Top line: None" in agent_demo.answer("What does x owe?", "x", "2026-10")


def test_main_answers_all_five(default_store, monkeypatch, capsys):
    _seed(default_store)
    monkeypatch.setattr(sys, "argv", ["agent_demo", "--customer", "acme", "--period", "2026-10"])
    agent_demo.main()
    assert "Result: 5/5 answered" in capsys.readouterr().out


def test_main_reports_errors(default_store, monkeypatch, capsys):
    def boom(*_a, **_k):
        raise RuntimeError("tool down")
    monkeypatch.setattr(agent_demo, "answer", boom)
    monkeypatch.setattr(sys, "argv", ["agent_demo"])
    agent_demo.main()
    out = capsys.readouterr().out
    assert "ERROR tool down" in out and "Result: 0/5 answered" in out
