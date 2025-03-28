import pickle
from typing import Any, Optional

from EntityHash import EntityHash, calc_hash
from overrides import overrides

from .object_cache import I_ItemProducer
from .ifaces import ProducerCallback, StoredItemID, I_CacheStorageModify


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
    def instantiate_item(
        self, data: bytes, extra_files: dict[str, StoredItemID] | None = None
    ) -> Any:
        if extra_files is not None:
            raise ValueError("Extra files are not supported for pickled items")
        item = pickle.loads(data)
        return item

    @overrides
    def serialize_item(self, item: Any) -> bytes:
        bytes = pickle.dumps(item)
        return bytes

    @overrides
    def propose_item_storage_key(self) -> Optional[StoredItemID]:
        return None


def pickle_wrap_promise(
    producer: ProducerCallback,
    serialization_performance_class: str = "",
    *args,
    **kwargs,
) -> I_PickledItemPromise:
    class PickledItemPromise(I_PickledItemPromise):
        promise: ProducerCallback
        args: tuple[Any, ...]
        kwargs: dict
        serialization_performance_class: str

        def __init__(
            self,
            item_key: EntityHash,
            promise: ProducerCallback,
            serialization_performance_class: str,
            args: tuple[Any, ...],
            kwargs: dict,
        ) -> None:
            super().__init__(item_key)
            self.promise = promise
            self.args = args
            self.kwargs = kwargs
            self.serialization_performance_class = serialization_performance_class

        @overrides
        def get_item_serialization_class(self) -> str:
            return self.serialization_performance_class

        @overrides
        def get_files_storing_state(
            self, storage: I_CacheStorageModify
        ) -> dict[str, StoredItemID]:
            return {}

        @overrides
        def protect_item(self):
            raise NotImplementedError()  # Shouldn't be called

        @overrides
        def compute_item(self) -> Any:
            return self.promise(*self.args, **self.kwargs)

    item_key = calc_hash({"_", args}.update(kwargs))

    return PickledItemPromise(
        item_key=item_key,
        promise=producer,
        serialization_performance_class=serialization_performance_class,
        args=args,
        kwargs=kwargs,
    )
