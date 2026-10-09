# Skill: billing-investigator
Use when answering "what did customer X consume / owe / why?" questions.

1. Call `get_usage(customer_id, period)` for quantities, `get_bill` for money.
2. If the user disputes a charge, call `explain_charge(customer_id, event_id)`
   and cite plan_version + contributing quantity.
3. If totals look wrong, call `reconcile(customer_id, period)` before speculating —
   report match/mismatches verbatim.
4. Never invent prices; read them from the bill lines. Never mutate anything.
