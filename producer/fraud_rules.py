import random
from faker import Faker

# Probabilities per iteration with sum < 1.0
P_HIGH_VALUE = 0.0125
P_RAPID_ACTIVITY = 0.01
P_GEO_ANOMALY = 0.005
P_REPEATED_HIGH = 0.02
P_CRYPTO_RISK = 0.075
P_VELOCITY_SPIKE = 0.01
# remaining = legit traffic

fake = Faker()


def rule1_high_value(build, _):
    """Single tx with amount > 1000, triggers Spark HIGH_VALUE rule."""
    return [build(amount=round(random.uniform(1000, 8000), 2))]


def rule2_rapid_activity(build, users):
    """4-7 txs, same user, rapid burst, triggers Spark RAPID_ACTIVITY rule."""
    user = random.choice(users)
    return [build(user_id=user) for _ in range(random.randint(4, 7))]


def rule3_geo_anomaly(build, users):
    """3-5 txs, same user, different cities, triggers Spark GEO_ANOMALY rule."""
    user = random.choice(users)
    cities = list({fake.city() for _ in range(10)})[: random.randint(3, 5)]
    return [build(user_id=user, location=c) for c in cities]


def rule4_repeated_high_value(build, users):
    """3-5 txs, same user, each > 1000, triggers Spark REPEATED_HIGH_VALUE rule."""
    user = random.choice(users)
    return [
        build(user_id=user, amount=round(random.uniform(1000, 6000), 2))
        for _ in range(random.randint(3, 5))
    ]


def rule5_crypto_risk(build, _):
    """Single tx, method=crypto + amount>500, triggers Spark CRYPTO_RISK rule."""
    return [build(method="crypto", amount=round(random.uniform(500, 5000), 2))]


def rule6_velocity_spike(build, users):
    """Burst where same user spends >5000 total, triggers Spark VELOCITY_SPIKE rule."""
    user = random.choice(users)
    total = 0
    batch = []
    while total < 5000:
        amt = round(random.uniform(1000, 4000), 2)
        total += amt
        batch.append(build(user_id=user, amount=amt))
    return batch


# Dispatcher


def generate_batch(build, users):
    """Return (list_of_txs, pattern_name) - pattern_name is for internal stats only."""
    r = random.random()
    acc = 0.0

    acc += P_HIGH_VALUE
    if r < acc:
        return (rule1_high_value(build, users), "HIGH_VALUE")

    acc += P_RAPID_ACTIVITY
    if r < acc:
        return (rule2_rapid_activity(build, users), "RAPID_ACTIVITY")

    acc += P_GEO_ANOMALY
    if r < acc:
        return (rule3_geo_anomaly(build, users), "GEO_ANOMALY")

    acc += P_REPEATED_HIGH
    if r < acc:
        return (rule4_repeated_high_value(build, users), "REPEATED_HIGH")

    acc += P_CRYPTO_RISK
    if r < acc:
        return (rule5_crypto_risk(build, users), "CRYPTO_RISK")

    acc += P_VELOCITY_SPIKE
    if r < acc:
        return (rule6_velocity_spike(build, users), "VELOCITY_SPIKE")

    return ([build()], "legitimate")
