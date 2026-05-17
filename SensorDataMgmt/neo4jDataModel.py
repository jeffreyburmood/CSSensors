""" this file contains the data models used to handle data processing with Neo4J """

from neo4j import SummaryCounters
from pydantic import BaseModel

class DBCounters(BaseModel):
    nodes_created: int=0
    nodes_deleted: int=0
    relationships_created: int=0
    relationships_deleted: int=0
    properties_set: int=0
    labels_added: int=0
    labels_removed: int=0
    contains_updates: bool=False

    def update_counts(self, summary_counts: SummaryCounters) -> None:
        self.nodes_created += summary_counts.nodes_created
        self.nodes_deleted += summary_counts.nodes_deleted
        self.relationships_created += summary_counts.relationships_created
        self.relationships_deleted += summary_counts.relationships_deleted
        self.properties_set += summary_counts.properties_set
        self.labels_added += summary_counts.labels_added
        self.labels_removed += summary_counts.labels_removed
