# GraphQL Mutations Documentation

This document provides examples for all available GraphQL mutations in Metamist.

## Table of Contents

- [Project Mutations](#project-mutations)

---

## Project Mutations

### Create Project

Create a new project with a specified name and dataset.

```graphql
mutation CreateProject($name: String!, $dataset: String!, $createTestProject: Boolean!) {
  project {
    createProject(
      name: $name
      dataset: $dataset
      createTestProject: $createTestProject
    ) {
      id
      name
      dataset
      meta
    }
  }
}
```

Variables:
```json
{
  "name": "my-new-project",
  "dataset": "my-dataset",
  "createTestProject": false
}
```

### Update Project

Update project metadata and settings.

```graphql
mutation UpdateProject($project: String!, $projectUpdateModel: JSON!) {
  project {
    updateProject(
      project: $project
      projectUpdateModel: $projectUpdateModel
    ) {
      id
      name
      meta
    }
  }
}
```

Variables:
```json
{
  "project": "my-project",
  "projectUpdateModel": {
    "meta": { "description": "Updated project description" }
  }
}
```

### Update Project Members

Add or update project member roles.

```graphql
mutation UpdateProjectMembers($project: String!, $members: [ProjectMemberUpdateInput!]!) {
  project {
    updateProjectMembers(
      project: $project
      members: $members
    ) {
      id
      name
      roles
    }
  }
}
```

Variables:
```json
{
  "project": "my-project",
  "members": [
    { "member": "user@example.com", "roles": ["reader", "contributor"] },
    { "member": "admin@example.com", "roles": ["project_admin"] }
  ]
}
```
