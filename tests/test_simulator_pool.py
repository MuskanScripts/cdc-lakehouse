import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

from simulator import ACTION_WEIGHTS, Pool, Simulator  # noqa: E402


def test_pool_add_remove_pick():
    p = Pool([1, 2, 3])
    p.add(2)  # duplicate ignored
    assert len(p) == 3
    p.remove(2)
    p.remove(99)  # missing id is a no-op
    assert len(p) == 2
    assert p.pick() in {1, 3}


def test_pool_pop_random_drains_without_duplicates():
    random.seed(0)
    items = list(range(100))
    p = Pool(items)
    popped = [p.pop_random() for _ in range(100)]
    assert sorted(popped) == items
    assert p.pop_random() is None


def test_every_action_is_implemented():
    for action in ACTION_WEIGHTS:
        assert callable(getattr(Simulator, action, None)), action
