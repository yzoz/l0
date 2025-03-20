from strawberry.fastapi import GraphQLRouter
from src.api.schema import schema

router = GraphQLRouter(schema)