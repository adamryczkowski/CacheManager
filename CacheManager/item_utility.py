from .ifaces import I_UtilityOfStoredItem, I_PersistentDB, DC_CacheItem
import datetime as dt


class ItemUtility(I_UtilityOfStoredItem):
    cost_of_minute_compute_rel_to_cost_of_1GB: float = 0.1  # e.g. Value of 10.0 means that 10 minute of compute time costs the same as holding 1GB of storage.
    reserved_free_space: int = (
        2**10
    )  # Reserved free space in GB excluded from the cache. E.g. 1.0 means the cache will not leave system with less than 1GB of free space.
    half_life_of_cache: float = 24.0  # Cache prunning strategy. The half-life of the value of cached items cache in hours. E.g. 24.0 means that the value of each cache item is halved every 24 hours.
    utility_of_1GB_free_space: float = 2  # The amount of free space that is considered as a cost of storing the cache item. E.g. 0.9 means that 10% of the free space is considered as a cost of storing the cache item.
    marginal_relative_utility_at_1GB: float = 1  # Shape parameter, equal to minus the derivative of the utility function at 1GB of free space divided by the utility at 1GB of free space. E.g. 2.0 means that the cost of storing the cache item at 1GB free space is rising 2 times faster than the cost of storing the cache item at 1GB of free space.

    def _calculate_disk_cost_of_new_item(
        self, free_space: int, size: int, existing: bool = False
    ) -> float:
        """
        Calculate the cost of storing the object in the cache.
        """
        size_float = size / (1024 * 1024 * 1024)  # Convert to GB
        free_space = free_space - self.reserved_free_space
        free_space_float = free_space / (1024 * 1024 * 1024)  # Convert to GB
        if existing:
            if free_space_float < 0:
                return -float("inf")
            utility_before = self._calculate_utility_of_free_space(
                free_space_float + size_float
            )
            utility_after = self._calculate_utility_of_free_space(free_space_float)
        else:
            if free_space_float < size_float:
                return -float("inf")
            utility_before = self._calculate_utility_of_free_space(free_space_float)
            utility_after = self._calculate_utility_of_free_space(
                free_space_float - size_float
            )
        return utility_before - utility_after

    def _calculate_utility_of_free_space(self, free_space: float) -> float:
        """
        Calculate the utility of the free space (measured in GB).
        """
        return self.utility_of_1GB_free_space * free_space ** (
            -self.marginal_relative_utility_at_1GB
        )

    def _calculate_decay_weight(self, age: float) -> float:
        """
        Calculate the weight of the object based on the age of the object in hours.
        """
        return 2 ** (-age / self.half_life_of_cache)

    def utility(
        self,
        item: DC_CacheItem,
        free_space: int,
        meta_db: I_PersistentDB,
        existing: bool = False,
        last_access_time: dt.datetime | None = None,
    ) -> float:
        if last_access_time is None:
            last_access_time = dt.datetime.now()
        item_age = (dt.datetime.now() - last_access_time).total_seconds() / 60.0

        positive_utility = (
            item.compute_time.total_seconds()
            / 60
            / self.cost_of_minute_compute_rel_to_cost_of_1GB
            * item.weight
            * self._calculate_decay_weight(item_age)
        )
        negative_cost = self._calculate_disk_cost_of_new_item(
            free_space=free_space, size=item.filesize, existing=existing
        )
        utility = positive_utility + negative_cost
        return utility
