import pickle
from pathlib import Path
from typing import Any, Optional

from EntityHash import EntityHash
from overrides import overrides

from . import I_AbstractItemID
from .object_cache import I_ItemProducer
from .ifaces import ProducerCallback


class I_PickledItemPromise(I_ItemProducer):
    item_key: EntityHash

    def __init__(
        self,
        item_key: EntityHash,
    ) -> None:
        assert isinstance(item_key, EntityHash)
        self.item_key = item_key

    @overrides
    def get_item_key(self) -> EntityHash:
        return self.item_key

    @overrides
    def instantiate_item(self, data: bytes) -> Any:
        item = pickle.loads(data)
        return item

    @overrides
    def serialize_item(self, item: Any) -> bytes:
        bytes = pickle.dumps(item)
        return bytes

    @overrides
    def propose_item_storage_key(self) -> Optional[Path | I_AbstractItemID]:
        return None


def pickle_wrap_promise(
    _item_key: EntityHash, _producer: ProducerCallback, *args, **kwargs
) -> I_PickledItemPromise:
    class PickledItemPromise(I_PickledItemPromise):
        promise: ProducerCallback
        args: tuple[Any, ...]
        kwargs: dict

        def __init__(
            self,
            item_key: EntityHash,
            promise: ProducerCallback,
            args: tuple[Any, ...],
            kwargs: dict,
        ) -> None:
            super().__init__(item_key)
            self.promise = promise
            self.args = args
            self.kwargs = kwargs

        @overrides
        def compute_item(self) -> Any:
            return self.promise(*self.args, **self.kwargs)

    return PickledItemPromise(_item_key, _producer, args, kwargs)
