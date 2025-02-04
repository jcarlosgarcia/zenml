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
"""SQL Model Implementations for Users, Teams, Roles."""
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel

from zenml.enums import PermissionType
from zenml.models import RoleAssignmentModel, RoleModel, TeamModel, UserModel
from zenml.zen_stores.schemas.project_schemas import ProjectSchema
from zenml.zen_stores.schemas.schema_utils import build_foreign_key_field

if TYPE_CHECKING:
    from zenml.zen_stores.schemas import (
        FlavorSchema,
        PipelineRunSchema,
        PipelineSchema,
        StackComponentSchema,
        StackSchema,
    )


class TeamAssignmentSchema(SQLModel, table=True):
    """SQL Model for team assignments."""

    __tablename__ = "team_assignment"

    user_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target="user",  # TODO: how to reference `UserSchema.__tablename__`?
        source_column="user_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
        primary_key=True,
    )
    team_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target="team",  # TODO: how to reference `TeamSchema.__tablename__`?
        source_column="team_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
        primary_key=True,
    )


class UserSchema(SQLModel, table=True):
    """SQL Model for users."""

    __tablename__ = "user"

    id: UUID = Field(primary_key=True)
    name: str
    full_name: str
    email: Optional[str] = Field(nullable=True)
    active: bool
    password: Optional[str] = Field(nullable=True)
    activation_token: Optional[str] = Field(nullable=True)
    created: datetime = Field(default_factory=datetime.now)
    updated: datetime = Field(default_factory=datetime.now)

    email_opted_in: Optional[bool] = Field(nullable=True)

    teams: List["TeamSchema"] = Relationship(
        back_populates="users", link_model=TeamAssignmentSchema
    )
    assigned_roles: List["UserRoleAssignmentSchema"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "delete"}
    )
    stacks: List["StackSchema"] = Relationship(
        back_populates="user",
    )
    components: List["StackComponentSchema"] = Relationship(
        back_populates="user",
    )
    flavors: List["FlavorSchema"] = Relationship(
        back_populates="user",
    )
    pipelines: List["PipelineSchema"] = Relationship(
        back_populates="user",
    )
    runs: List["PipelineRunSchema"] = Relationship(
        back_populates="user",
    )

    @classmethod
    def from_create_model(cls, model: UserModel) -> "UserSchema":
        """Create a `UserSchema` from a `UserModel`.

        Args:
            model: The `UserModel` from which to create the schema.

        Returns:
            The created `UserSchema`.
        """
        return cls(
            id=model.id,
            name=model.name,
            full_name=model.full_name,
            active=model.active,
            password=model.get_hashed_password(),
            activation_token=model.get_hashed_activation_token(),
        )

    def from_update_model(self, model: UserModel) -> "UserSchema":
        """Update a `UserSchema` from a `UserModel`.

        Args:
            model: The `UserModel` from which to update the schema.

        Returns:
            The updated `UserSchema`.
        """
        self.name = model.name
        self.full_name = model.full_name
        self.active = model.active
        self.password = model.get_hashed_password()
        self.activation_token = model.get_hashed_activation_token()
        self.updated = datetime.now()
        return self

    def to_model(self) -> UserModel:
        """Convert a `UserSchema` to a `UserModel`.

        Returns:
            The converted `UserModel`.
        """
        return UserModel(
            id=self.id,
            name=self.name,
            full_name=self.full_name,
            email=self.email,
            email_opted_in=self.email_opted_in,
            active=self.active,
            password=self.password,
            activation_token=self.activation_token,
            created=self.created,
            updated=self.updated,
        )


class TeamSchema(SQLModel, table=True):
    """SQL Model for teams."""

    __tablename__ = "team"

    id: UUID = Field(primary_key=True)
    name: str
    created: datetime = Field(default_factory=datetime.now)
    updated: datetime = Field(default_factory=datetime.now)

    users: List["UserSchema"] = Relationship(
        back_populates="teams", link_model=TeamAssignmentSchema
    )
    assigned_roles: List["TeamRoleAssignmentSchema"] = Relationship(
        back_populates="team", sa_relationship_kwargs={"cascade": "delete"}
    )

    @classmethod
    def from_create_model(cls, model: TeamModel) -> "TeamSchema":
        """Create a `TeamSchema` from a `TeamModel`.

        Args:
            model: The `TeamModel` from which to create the schema.

        Returns:
            The created `TeamSchema`.
        """
        return cls(id=model.id, name=model.name)

    def from_update_model(self, model: TeamModel) -> "TeamSchema":
        """Update a `TeamSchema` from a `TeamModel`.

        Args:
            model: The `TeamModel` from which to update the schema.

        Returns:
            The updated `TeamSchema`.
        """
        self.name = model.name
        self.updated = datetime.now()
        return self

    def to_model(self) -> TeamModel:
        """Convert a `TeamSchema` to a `TeamModel`.

        Returns:
            The converted `TeamModel`.
        """
        return TeamModel(
            id=self.id,
            name=self.name,
            created=self.created,
            updated=self.updated,
        )


class RoleSchema(SQLModel, table=True):
    """SQL Model for roles."""

    __tablename__ = "role"

    id: UUID = Field(primary_key=True)
    name: str
    created: datetime = Field(default_factory=datetime.now)
    updated: datetime = Field(default_factory=datetime.now)
    permissions: List["RolePermissionSchema"] = Relationship(
        back_populates="roles", sa_relationship_kwargs={"cascade": "delete"}
    )
    user_role_assignments: List["UserRoleAssignmentSchema"] = Relationship(
        back_populates="role", sa_relationship_kwargs={"cascade": "delete"}
    )
    team_role_assignments: List["TeamRoleAssignmentSchema"] = Relationship(
        back_populates="role", sa_relationship_kwargs={"cascade": "delete"}
    )

    @classmethod
    def from_create_model(cls, model: RoleModel) -> "RoleSchema":
        """Create a `RoleSchema` from a `RoleModel`.

        Args:
            model: The `RoleModel` from which to create the schema.

        Returns:
            The created `RoleSchema`.
        """
        return cls(id=model.id, name=model.name)

    def from_update_model(self, model: RoleModel) -> "RoleSchema":
        """Update a `RoleSchema` from a `RoleModel`.

        Args:
            model: The `RoleModel` from which to update the schema.

        Returns:
            The updated `RoleSchema`.
        """
        self.name = model.name
        self.updated = datetime.now()
        return self

    def to_model(self) -> RoleModel:
        """Convert a `RoleSchema` to a `RoleModel`.

        Returns:
            The converted `RoleModel`.
        """
        return RoleModel(
            id=self.id,
            name=self.name,
            created=self.created,
            updated=self.updated,
            permissions=[PermissionType(p.name) for p in self.permissions],
        )


class UserRoleAssignmentSchema(SQLModel, table=True):
    """SQL Model for assigning roles to users for a given project."""

    __tablename__ = "user_role_assignment"

    id: UUID = Field(primary_key=True, default_factory=uuid4)
    role_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target=RoleSchema.__tablename__,
        source_column="role_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
    )
    user_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target=UserSchema.__tablename__,
        source_column="user_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
    )
    project_id: Optional[UUID] = build_foreign_key_field(
        source=__tablename__,
        target=ProjectSchema.__tablename__,
        source_column="project_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=True,
    )
    created: datetime = Field(default_factory=datetime.now)
    updated: datetime = Field(default_factory=datetime.now)

    role: RoleSchema = Relationship(back_populates="user_role_assignments")
    user: UserSchema = Relationship(back_populates="assigned_roles")
    project: Optional["ProjectSchema"] = Relationship(
        back_populates="user_role_assignments"
    )

    def to_model(self) -> RoleAssignmentModel:
        """Convert a `UserRoleAssignmentSchema` to a `RoleAssignmentModel`.

        Returns:
            The converted `RoleAssignmentModel`.
        """
        return RoleAssignmentModel(
            id=self.id,
            role=self.role_id,
            user=self.user_id,
            project=self.project_id,
            created=self.created,
            updated=self.updated,
        )


class TeamRoleAssignmentSchema(SQLModel, table=True):
    """SQL Model for assigning roles to teams for a given project."""

    __tablename__ = "team_role_assignment"

    id: UUID = Field(primary_key=True, default_factory=uuid4)
    role_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target=RoleSchema.__tablename__,
        source_column="role_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
    )
    team_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target=TeamSchema.__tablename__,
        source_column="team_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
    )
    project_id: Optional[UUID] = build_foreign_key_field(
        source=__tablename__,
        target=ProjectSchema.__tablename__,
        source_column="project_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=True,
    )
    created: datetime = Field(default_factory=datetime.now)
    updated: datetime = Field(default_factory=datetime.now)

    role: RoleSchema = Relationship(back_populates="team_role_assignments")
    team: TeamSchema = Relationship(back_populates="assigned_roles")
    project: Optional["ProjectSchema"] = Relationship(
        back_populates="team_role_assignments"
    )

    def to_model(self) -> RoleAssignmentModel:
        """Convert a `TeamRoleAssignmentSchema` to a `RoleAssignmentModel`.

        Returns:
            The converted `RoleAssignmentModel`.
        """
        return RoleAssignmentModel(
            id=self.id,
            role=self.role_id,
            team=self.team_id,
            project=self.project_id,
            created=self.created,
            updated=self.updated,
        )


class RolePermissionSchema(SQLModel, table=True):
    """SQL Model for team assignments."""

    __tablename__ = "role_permission"

    name: PermissionType = Field(primary_key=True)
    role_id: UUID = build_foreign_key_field(
        source=__tablename__,
        target=RoleSchema.__tablename__,
        source_column="role_id",
        target_column="id",
        ondelete="CASCADE",
        nullable=False,
        primary_key=True,
    )
    roles: List["RoleSchema"] = Relationship(back_populates="permissions")
