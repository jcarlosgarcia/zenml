#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""SQL Model Implementations for Stacks."""

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID

from sqlmodel import Field, Relationship, SQLModel

from zenml.zen_stores.schemas.base_schemas import ShareableSchema
from zenml.zen_stores.schemas.schema_utils import build_foreign_key_field

if TYPE_CHECKING:
    from zenml.zen_stores.schemas import (
        PipelineRunSchema,
        StackComponentSchema,
        UserSchema,
    )


class StackCompositionSchema(SQLModel, table=True):
    """SQL Model for stack definitions.

    Join table between Stacks and StackComponents.
    """

    __tablename__ = "stack_composition"

    stack_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target="stack",  # TODO: how to reference `StackSchema.__tablename__`?
        source_column="stack_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
        primary_key=True,
    )
    component_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target="stack_component",  # TODO: how to reference `StackComponentSchema.__tablename__`?
        source_column="component_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
        primary_key=True,
    )


class StackSchema(ShareableSchema, table=True):
    """SQL Model for stacks."""

    __tablename__ = "stack"

    id: UUID = Field(primary_key=True)
    created: datetime = Field(default_factory=datetime.now)
    updated: datetime = Field(default_factory=datetime.now)

    name: str
    is_shared: bool

    project_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target=ProjectSchema.__tablename__,
        source_column="project_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
    )
    project: "ProjectSchema" = Relationship(back_populates="stacks")

    user_id: Optional[UUID] = build_foreign_key_field(
        source=__tablename__,
        target=UserSchema.__tablename__,
        source_column="user_id",
        target_column="id",
        ondelete="SET NULL",
        nullable=True,
    )
    user: "UserSchema" = Relationship(back_populates="stacks")

    components: List["StackComponentSchema"] = Relationship(
        back_populates="stacks",
        link_model=StackCompositionSchema,
    )
    runs: List["PipelineRunSchema"] = Relationship(back_populates="stack")

    def update(
        self,
        stack_update: StackUpdateModel,
        components: List["StackComponentSchema"],
    ):
        for field, value in stack_update.dict(exclude_unset=True).items():
            if field == "components":
                self.components = components

            elif field == "user":
                assert self.user_id == value

            elif field == "project":
                assert self.project_id == value

            else:
                setattr(self, field, value)

        self.updated = datetime.now()
        return self

    def to_model(self) -> StackResponseModel:
        """Creates a `HydratedStackModel` from an instance of a 'StackSchema'.

        Returns:
            a 'HydratedStackModel'.
        """
        return StackResponseModel(
            id=self.id,
            name=self.name,
            user=self.user.to_model(),
            project=self.project.to_model(),
            is_shared=self.is_shared,
            components={c.type: [c.to_model()] for c in self.components},
            created=self.created,
            updated=self.updated,
        )
