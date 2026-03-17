"""Neo4j connection client for EE-OpenGraph ETL pipelines."""

import logging
import os
from typing import Any

from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver, Result

load_dotenv()

logger = logging.getLogger(__name__)


class Neo4jClient:
    """Thin wrapper around the Neo4j bolt driver.

    Args:
        uri: Bolt URI, defaults to NEO4J_URI env var.
        user: Username, defaults to NEO4J_USER env var.
        password: Password, defaults to NEO4J_PASSWORD env var.
    """

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self._uri = uri or os.environ["NEO4J_URI"]
        self._user = user or os.environ["NEO4J_USER"]
        self._password = password or os.environ["NEO4J_PASSWORD"]
        self._driver: Driver | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the driver connection and verify connectivity."""
        self._driver = GraphDatabase.driver(
            self._uri, auth=(self._user, self._password)
        )
        self._driver.verify_connectivity()
        logger.info("Connected to Neo4j at %s", self._uri)

    def close(self) -> None:
        """Close the driver, releasing all connections."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j connection closed")

    def __enter__(self) -> "Neo4jClient":
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Core query
    # ------------------------------------------------------------------

    def run_query(
        self,
        cypher: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query and return all records as dicts.

        Args:
            cypher: Cypher query string.
            parameters: Optional parameter mapping.

        Returns:
            List of record dicts.
        """
        if self._driver is None:
            raise RuntimeError("Not connected — call connect() first")
        with self._driver.session() as session:
            result: Result = session.run(cypher, parameters or {})
            return [record.data() for record in result]

    def run_query_batch(
        self,
        cypher: str,
        rows: list[dict[str, Any]],
        batch_size: int = 500,
    ) -> int:
        """Execute a Cypher query in batches using UNWIND.

        The query must use ``$rows`` as the parameter name for the batch list.

        Args:
            cypher: Cypher query using UNWIND $rows AS row …
            rows: Full list of parameter dicts.
            batch_size: Records per transaction.

        Returns:
            Total number of rows processed.
        """
        if self._driver is None:
            raise RuntimeError("Not connected — call connect() first")
        total = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            with self._driver.session() as session:
                session.run(cypher, {"rows": chunk})
            total += len(chunk)
            logger.debug("Batch processed %d / %d rows", total, len(rows))
        return total

    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------

    def merge_node(
        self,
        label: str,
        match_props: dict[str, Any],
        set_props: dict[str, Any] | None = None,
    ) -> None:
        """MERGE a node by match_props, then SET additional properties.

        Args:
            label: Node label (e.g. "Person", "Company").
            match_props: Properties used in the MERGE clause.
            set_props: Additional properties written on create or update.
        """
        set_clause = ""
        params: dict[str, Any] = {"match_props": match_props}
        if set_props:
            set_clause = "SET n += $set_props"
            params["set_props"] = set_props

        cypher = f"MERGE (n:{label} $match_props) {set_clause}"
        self.run_query(cypher, params)

    def merge_relationship(
        self,
        from_label: str,
        from_match: dict[str, Any],
        rel_type: str,
        to_label: str,
        to_match: dict[str, Any],
        rel_props: dict[str, Any] | None = None,
    ) -> None:
        """MERGE a relationship between two existing nodes.

        Both nodes must already exist (or be MERGE-able by from_match /
        to_match).

        Args:
            from_label: Label of the source node.
            from_match: Properties to match the source node.
            rel_type: Relationship type (e.g. "DONATED_TO").
            to_label: Label of the target node.
            to_match: Properties to match the target node.
            rel_props: Properties to set on the relationship.
        """
        set_clause = ""
        params: dict[str, Any] = {
            "from_match": from_match,
            "to_match": to_match,
        }
        if rel_props:
            set_clause = "SET r += $rel_props"
            params["rel_props"] = rel_props

        cypher = (
            f"MERGE (a:{from_label} $from_match) "
            f"MERGE (b:{to_label} $to_match) "
            f"MERGE (a)-[r:{rel_type}]->(b) "
            f"{set_clause}"
        )
        self.run_query(cypher, params)
