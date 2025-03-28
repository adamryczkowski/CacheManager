from typing import Any, Optional
from typing import Type

from EntityHash import EntityHash, calc_hash
from overrides import overrides
from pydantic import BaseModel, TypeAdapter

from .ifaces import ProducerCallback, StoredItemID, I_CacheStorageModify

from .object_cache import I_ItemProducer


class I_JSONItemPromise(I_ItemProducer):
    item_key: EntityHash
    item_type: Type[BaseModel]

    def __init__(self, item_key: EntityHash, item_type: Type[BaseModel]) -> None:
        assert isinstance(item_key, EntityHash)
        assert issubclass(item_type, BaseModel)
        self.item_key = item_key
        self.item_type = item_type

    @overrides
    def get_item_key(self) -> EntityHash:
        return self.item_key

    @overrides
    def instantiate_item(
        self, data: bytes, extra_files: dict[str, StoredItemID] | None = None
    ) -> Any:
        if extra_files is not None:
            raise ValueError(
                "Extra files are not supported for JSON-serialized objects"
            )
        json = data.decode()
        item = TypeAdapter(self.item_type).validate_json(json)
        return item

    @overrides
    def serialize_item(self, item: Any) -> bytes:
        assert isinstance(item, self.item_type)
        return item.model_dump_json().encode()
        item.verify()

    @overrides
    def propose_item_storage_key(self) -> Optional[StoredItemID]:
        return None


def json_wrap_promise(
    item_type: Type[BaseModel],
    producer: ProducerCallback,
    serialization_performance_class: str = "",
    *args,
    **kwargs,
) -> I_JSONItemPromise:
    """
    Wraps a producer function into a JSONItemPromise.
    :param item_type: Type of the item to be produced. Must be a subclass of BaseModel.
    :param producer: The function that delivers the result, given the arguments.
    :param serialization_performance_class: leave it empty to use the default serialization performance class.
    No point of using other performance class than the default for JSON serialization - the performance of JSON serialization
    given the object size should be constant.
    :param args: arguments to the producer
    :param kwargs: arguments to the producer
    :return:
    """

    class JSONItemPromise(I_JSONItemPromise):
        promise: ProducerCallback
        args: tuple[Any, ...]
        kwargs: dict
        serialization_performance_class: str

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
            raise NotImplementedError()  # Shouldn't be called.

        def __init__(
            self,
            item_key: EntityHash,
            item_type: Type[BaseModel],
            promise: ProducerCallback,
            serialization_performance_class: str,
            args: tuple[Any, ...],
            kwargs: dict,
        ) -> None:
            super().__init__(item_key=item_key, item_type=item_type)
            # TODO In future we may try to deduce the item_type from the signature of the producer.
            self.promise = promise
            self.args = args
            self.kwargs = kwargs
            self.serialization_performance_class = serialization_performance_class

        @overrides
        def compute_item(self) -> Any:
            return self.promise(*self.args, **self.kwargs)

    item_key = calc_hash({"_", args}.update(kwargs))

    return JSONItemPromise(
        item_key=item_key,
        item_type=item_type,
        promise=producer,
        serialization_performance_class=serialization_performance_class,
        args=args,
        kwargs=kwargs,
    )
