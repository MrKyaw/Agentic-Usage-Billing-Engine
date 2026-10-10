"""MCP tools: in-process via FastMCP.call_tool, and over the real stdio protocol."""
import json
import os
import sys

import pytest
from conftest import SRC, make_event

from billing_engine import mcp_server
from billing_engine.ingest import ingest_one
from billing_engine.processor import drain

pytestmark = pytest.mark.req("FR7")


def _seed(store):
    ingest_one(store, make_event("m-1", quantity=100))
    ingest_one(store, make_event("m-2", metric="render_minutes", quantity=1100))
    drain(store)


def _payload(result):
    content = result[0] if isinstance(result, tuple) else result
    return json.loads(content[0].text)


async def test_tools_registered():
    names = {t.name for t in await mcp_server.mcp.list_tools()}
    assert names == {"get_usage", "get_bill", "reconcile", "explain_charge"}


async def test_call_tools_in_process(default_store):
    _seed(default_store)
    mcp = mcp_server.mcp
    usage = _payload(await mcp.call_tool("get_usage",
                                         {"customer_id": "acme", "period": "2026-10"}))
    assert {line["metric"] for line in usage["lines"]} == {"cloud_credits", "render_minutes"}
    bill = _payload(await mcp.call_tool("get_bill",
                                        {"customer_id": "acme", "period": "2026-10"}))
    assert bill["total_amount"] == 60.0  # 100*.10 + 1000 billable*.05
    rec = _payload(await mcp.call_tool("reconcile", {}))
    assert rec["match"] and rec["checked"] == 2
    scoped = _payload(await mcp.call_tool("reconcile",
                                          {"customer_id": "acme", "period": "2026-10"}))
    assert scoped["match"]
    ex = _payload(await mcp.call_tool("explain_charge",
                                      {"customer_id": "acme", "event_id": "m-1"}))
    assert ex["found"] and ex["period_bill_line"]["metric"] == "cloud_credits"
    assert ex["bill_total"] == 60.0 and "qty=100.0" in ex["note"]


async def test_explain_charge_not_found_or_wrong_customer(default_store):
    _seed(default_store)
    assert mcp_server.explain_charge("acme", "missing") == {
        "found": False, "event_id": "missing", "customer_id": "acme"}
    assert mcp_server.explain_charge("globex", "m-1")["found"] is False


@pytest.mark.e2e
async def test_stdio_protocol_round_trip(default_store, tmp_path):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    _seed(default_store)
    env = {**os.environ, "PYTHONPATH": str(SRC),
           "BILLING_DB_PATH": default_store.db_path,
           "BILLING_RAW_LOG_DIR": str(default_store.raw_log_dir)}
    params = StdioServerParameters(command=sys.executable,
                                   args=["-m", "billing_engine.mcp_server"], env=env)
    with open(tmp_path / "mcp-stderr.log", "w") as errlog:
        async with stdio_client(params, errlog=errlog) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = {t.name for t in (await session.list_tools()).tools}
                assert tools == {"get_usage", "get_bill", "reconcile", "explain_charge"}
                res = await session.call_tool("get_bill",
                                              {"customer_id": "acme", "period": "2026-10"})
                assert not res.isError
                assert json.loads(res.content[0].text)["total_amount"] == 60.0
                rec = await session.call_tool("reconcile", {})
                assert json.loads(rec.content[0].text)["match"] is True
