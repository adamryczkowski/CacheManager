from typing import Any
from typing import Type

from EntityHash import EntityHash
from overrides import overrides
from pydantic import BaseModel, TypeAdapter
from .ifaces import ProducerCallback

from .object_cache import I_ItemProducer


class I_JSONItemPromise(I_ItemProducer):
    item_key: EntityHash
    item_type: Type[BaseModel]

    def __init__(self, item_key: EntityHash, item_type: Type[BaseModel]) -> None:
        assert isinstance(item_key, EntityHash)
        assert issubclass(item_type, BaseModel)
        self.item_key = item_key

    @overrides
    def get_item_key(self) -> EntityHash:
        return self.item_key

    @overrides
    def instantiate_item(self, data: bytes) -> Any:
        json = data.decode()
        item = TypeAdapter(self.item_type).validate_json(json)
        return item

    @overrides
    def serialize_item(self, item: Any) -> bytes:
        assert isinstance(item, self.item_type)
        return item.model_dump_json().encode()


def json_wrap_promise(
    _item_key: EntityHash, _producer: ProducerCallback, *args, **kwargs
) -> I_JSONItemPromise:
    class PickledItemPromise(I_JSONItemPromise):
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
