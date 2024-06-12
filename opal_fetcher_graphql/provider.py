import graphene

from PydanticObjectType import PydanticObjectType
from graphene import Field, ObjectType, String
from pydantic import BaseModel

from opal_common.fetcher.fetch_provider import BaseFetchProvider
from opal_common.fetcher.events import FetcherConfig, FetchEvent
from opal_common.logger import logger

from typing import Optional, List


class GraphQLFetcherConfig(FetcherConfig):
    """
    Config for GraphQLFetchProvider, inherits from `FetcherConfig`.
    * In your own class, you must set the value of the `fetcher` key to be your custom provider class name.
    """
    fetcher: str = "GraphQLFetchProvider"

    BaseModel = Field(..., description="the Base Data Model to be implemented and also ObjectType")

    schema = graphene.Schema(
        ..., description="the schema Method to be initialized from GraphQL class"
    )

    query = Field(
        ..., description="the query to run against GraphQL Schema"

    )




class GraphQLFetchEvent(FetchEvent):
    """
    When writing a custom provider, you must create a custom FetchEvent subclass, just like this class.
    In your own class, you must:
    * set the value of the `fetcher` key to be your custom provider class name.
    * set the type of the `config` key to be your custom config class (the one just above).
    """
    fetcher: str = "GraphQLFetchProvider"
    config: GraphQLFetcherConfig = None


class GraphQLFetchProvider(BaseFetchProvider):
  """
    An OPAL fetch provider for GraphQL.

    We fetch data from a GraphQL Schema by running a query,
    transforming the results to json and dumping the results into the policy store.

    When writing a custom provider, you must:
    - derive your provider class (inherit) from BaseFetchProvider
    - create a custom config class, as shown above, that derives from FetcherConfig
    - create a custom event class, as shown above, that derives from FetchEvent

    At minimum, your custom provider class must implement:
    - __init__() - and call super().__init__(event)
    - parse_event() - this method gets a `FetchEvent` object and must transform this object to *your own custom event class*.
        - Notice that `FetchEvent` is the base class
        - Notice that `GraphQLFetchEvent` is the custom event class
    - _fetch_() - your custom fetch method, can use the data from your event
    and config to figure out *what and how to fetch* and actually do it.
    - _process_() - if your fetched data requires some processing, you should do it here.
        - The return type from this method must be json-able, i.e: can be serialized to json.

    You may need to implement:
    - __aenter__() - if your provider has state that needs to be cleaned up,
    (i.e: http session, postgres connection, etc) the state may be initialized in this method.
    - __aexit__() - if you initialized stateful objects (i.e: acquired resources) in your __aenter__, you must release them in __aexit__
    """
    RETRY_CONFIG = {
        "wait": wait.wait_random_exponential(),
        "stop": stop.stop_after_attempt(10),
        "retry": retry_unless_exception_type(
            DataError
        ),  # query error (i.e: invalid schema, etc)
        "reraise": True,
    }

    def __init__(self, event: GraphQLFetcherConfig) -> None:
        if event.config is None:
            event.config = GraphQLFetcherConfig()
        super().__init__(event)
        self.schema = None
        self.query = None

    def parse_event(self, event: FetchEvent) -> GraphQLFetchEvent:
        return GraphQLFetchEvent(**event.dict(exclude={"config"}), config=event.config)


    async def _fetch_(self):
        self._event: GraphQLFetchEvent  # type casting

        if self._event.config is None:
            logger.warning(
                "incomplete fetcher config: GraphQL schema require a query to specify what data to fetch!"
            )
            return

        logger.debug(f"{self.__class__.__name__} fetching from {self._url}")

        if self._event.config:
            row = await self.schema.execute(self._event.config.query)
            return [row]
        else:
            return await self.schema.execute(self._event.config.query)

    async def _process_(self, records):
        self._event: GraphQLFetchEvent  # type casting

        
        if self._event.config:
            if records and len(records) > 0:
           
                return (records.data)
            else:
                return {}
        