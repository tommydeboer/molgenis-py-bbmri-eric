import random
import secrets
import string
import time
import timeit
import uuid
from statistics import mean, stdev

import shortuuid

alphabet = string.ascii_lowercase + string.digits
su = shortuuid.ShortUUID(alphabet=alphabet)

shortuuid.set_alphabet("23")


def random_choice():
    return "".join(random.choices(alphabet, k=8))


def truncated_uuid4():
    return str(uuid.uuid4())[:8]


def shortuuid_random():
    return su.random(length=8)


def secrets_random_choice():
    return "".join(secrets.choice(alphabet) for _ in range(8))


def shortuuid_custom():
    return shortuuid.uuid()[:12]


def secrets_hex():
    return secrets.token_hex(6)


def check_collisions(fun):
    out = set()
    count = 0
    for _ in range(100_000_000):
        new = fun()
        if new in out:
            count += 1
        else:
            out.add(new)
    return count


def run_and_print_results(fun):
    round_digits = 5
    now = time.time()
    collisions = check_collisions(fun)
    total_time = round(time.time() - now, round_digits)

    trials = 1_000
    runs = 100
    func_time = timeit.repeat(fun, repeat=runs, number=trials)
    avg = round(mean(func_time), round_digits)
    std = round(stdev(func_time), round_digits)

    print(
        f"{fun.__name__}: collisions {collisions} - "
        f"time (s) {avg} ± {std} - "
        f"total (s) {total_time}"
    )


if __name__ == "__main__":
    # run_and_print_results(random_choice)
    # run_and_print_results(truncated_uuid4)
    # run_and_print_results(shortuuid_random)
    # run_and_print_results(secrets_random_choice)
    run_and_print_results(secrets_hex)
