import json

import pandas as pd


def is_instagram_data(v_l: str):
    for v in json.loads(v_l):
        if "instagram" == v.get("name"):
            return True
    return False


def calculate(v):
    service_type = 2
    revenue_from_b = 0
    revenue_from_c = 0

    register_count = max(v[2] or 0, v[3] or 0)

    level2, level3 = v[0], v[1]
    if "# credentails" == level3:
        service_type = 3
    elif "# credentails" == level2:
        service_type = 3
    elif "# credentails" == level2:
        service_type = 1
    elif "# credentails" == level3:
        service_type = 1
    elif "# credentails" == level3:
        service_type = 1
    elif "# credentails" == level3:
        service_type = 1

    if service_type == 1:
        register_count_key = 0
        is_instagram_need = v[4]

        # 있으면 더하고 아니면 더하지 않는다.
        if "# credentails" == level3:
            if register_count >= 10 and register_count <= 100:
                register_count_key = 1
            elif register_count > 100 and register_count <= 300:
                register_count_key = 2
            elif register_count > 300 and register_count <= 500:
                register_count_key = 3
            elif register_count > 500:
                register_count_key = 4
            info = {
                1: {"N": 3000000, "M": 1000000},
                2: {"N": 4000000, "M": 1500000},
                3: {"N": 5000000, "M": 2000000},
                4: {"N": 5000000, "M": 2000000},
            }

        elif "# credentails" == level3:
            if register_count >= 300 and register_count < 500:
                register_count_key = 1
            elif register_count >= 500 and register_count < 1000:
                register_count_key = 2
            elif register_count >= 1000 and register_count < 2000:
                register_count_key = 3
            elif register_count >= 2000:
                register_count_key = 4
            info = {
                1: {"N": 15000, "M": 4000},
                2: {"N": 13000, "M": 4000},
                3: {"N": 10000, "M": 4000},
                4: {"N": 8000, "M": 3200},
            }

        elif "# credentails" == level3:
            if register_count >= 500 and register_count < 1000:
                register_count_key = 1
            elif register_count >= 1000 and register_count < 3000:
                register_count_key = 2
            elif register_count >= 3000 and register_count < 4000:
                register_count_key = 3
            elif register_count >= 4000:
                register_count_key = 4
            info = {
                1: {"N": 10000, "M": 3000},
                2: {"N": 8000, "M": 3000},
                3: {"N": 6000, "M": 3000},
                4: {"N": 4800, "M": 2400},
            }

        if register_count_key in [1, 2, 3, 4]:
            info = info.get(register_count_key, {})
            n = info.get("N", 0)
            m = info.get("M", 0) if is_instagram_need else 0
            revenue_from_b = n + m

    margin = float(v[5]) if not pd.isna(v[5]) else 0
    margin = float(margin / register_count) if (register_count > 0) else 0
    product_fee = float(v[6]) if not pd.isna(v[6]) else 0

    revenue_from_c = margin + product_fee

    return pd.Series(
        [
            service_type,
            service_type == 1,
            service_type == 3,
            revenue_from_b,
            revenue_from_c,
            revenue_from_b + revenue_from_c,
        ]
    )
